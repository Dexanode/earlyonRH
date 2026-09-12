"""Build ordered buyer flow and conservative funding clusters from local evidence."""
import argparse
from collections import defaultdict
import json
import logging
import sqlite3
import time

from listener import database, now, set_meta

LOG=logging.getLogger('capital-flow')


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS capital_wallet_asset(
        asset TEXT NOT NULL,wallet TEXT NOT NULL,first_buy_block INTEGER,first_buy_time INTEGER,
        last_buy_block INTEGER,last_buy_time INTEGER,buy_count INTEGER NOT NULL,sell_count INTEGER NOT NULL,
        first_buy_raw TEXT,last_buy_raw TEXT,largest_buy_raw TEXT,repeat_latency_seconds INTEGER,
        size_trend REAL,acquired_raw TEXT,sold_raw TEXT,retained_raw TEXT,
        funding_root TEXT,funding_evidence TEXT,funding_confidence TEXT,updated_at TEXT NOT NULL,
        PRIMARY KEY(asset,wallet));
      CREATE INDEX IF NOT EXISTS capital_asset ON capital_wallet_asset(asset);
      CREATE TABLE IF NOT EXISTS capital_clusters(
        asset TEXT NOT NULL,funding_root TEXT NOT NULL,members INTEGER NOT NULL,buys INTEGER NOT NULL,
        retained_raw TEXT,confidence TEXT NOT NULL,updated_at TEXT NOT NULL,
        PRIMARY KEY(asset,funding_root));
      CREATE TABLE IF NOT EXISTS capital_migrations(
        target_asset TEXT NOT NULL,wallet TEXT NOT NULL,source_asset TEXT NOT NULL,
        source_sell_time INTEGER,target_buy_time INTEGER,latency_seconds INTEGER,
        source_sold_raw TEXT,target_buy_raw TEXT,confidence TEXT NOT NULL,updated_at TEXT NOT NULL,
        PRIMARY KEY(target_asset,wallet,source_asset));
      CREATE INDEX IF NOT EXISTS capital_migration_target ON capital_migrations(target_asset);
    ''')


def rebuild(db,limit=50000):
    schema(db)
    attrs={r['tx_hash']:dict(r) for r in db.execute("SELECT * FROM tx_attributions WHERE error IS NULL")} if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='tx_attributions'").fetchone() else {}
    rows=db.execute("SELECT tx_hash,asset,name,decoded,block_number,event_timestamp FROM events WHERE asset IS NOT NULL AND name IN ('CurveBuy','CurveSell','DexBuy','DexSell') ORDER BY block_number DESC,log_index DESC LIMIT ?",(limit,)).fetchall()[::-1]
    flows=defaultdict(lambda:{'buys':[],'sells':[]})
    for row in rows:
        v=json.loads(row['decoded']);buy=row['name'] in ('CurveBuy','DexBuy');a=attrs.get(row['tx_hash'])
        wallet=(a.get('sender') if row['name'].startswith('Dex') and a else None) or (v.get('buyer') if buy else v.get('seller'))
        if not wallet:continue
        raw=int(v.get('quoteIn') or 0) if buy else int(v.get('quoteOut') or 0)
        tokens=int(v.get('tokensOut') or 0) if buy else int(v.get('tokensIn') or 0)
        flows[(row['asset'],wallet.lower())]['buys' if buy else 'sells'].append((row,raw,tokens))
    stamp=now();clusters=defaultdict(lambda:{'wallets':set(),'buys':0,'retained':0,'confidence':'provisional'});migrations=[]
    with db:
        db.execute('DELETE FROM capital_wallet_asset');db.execute('DELETE FROM capital_clusters');db.execute('DELETE FROM capital_migrations')
        for (asset,wallet),f in flows.items():
            if not f['buys']:continue
            buys=f['buys'];sells=f['sells'];first,last=buys[0],buys[-1]
            attr=attrs.get(first[0]['tx_hash'],{});relation=attr.get('relation');sender=(attr.get('sender') or '').lower()
            root=sender if relation=='routed' and sender else wallet
            confidence='observed-shared-sender' if relation=='routed' and sender else 'provisional-self'
            evidence=first[0]['tx_hash'] if attr else None
            acquired=sum(x[2] for x in buys);sold=sum(x[2] for x in sells);retained=max(0,acquired-sold)
            latency=(buys[1][0]['event_timestamp']-first[0]['event_timestamp']) if len(buys)>1 and buys[1][0]['event_timestamp'] and first[0]['event_timestamp'] else None
            trend=round(last[1]/first[1],3) if first[1] else None
            db.execute('INSERT INTO capital_wallet_asset VALUES('+','.join('?'*20)+')',(asset,wallet,first[0]['block_number'],first[0]['event_timestamp'],last[0]['block_number'],last[0]['event_timestamp'],len(buys),len(sells),str(first[1]),str(last[1]),str(max(x[1] for x in buys)),latency,trend,str(acquired),str(sold),str(retained),root,evidence,confidence,stamp))
            c=clusters[(asset,root)];c['wallets'].add(wallet);c['buys']+=len(buys);c['retained']+=retained;c['confidence']=confidence
        for (asset,root),c in clusters.items():
            db.execute('INSERT INTO capital_clusters VALUES(?,?,?,?,?,?,?)',(asset,root,len(c['wallets']),c['buys'],str(c['retained']),c['confidence'],stamp))
        by_wallet=defaultdict(list)
        for (asset,wallet),f in flows.items():
            if f['buys']:by_wallet[wallet].append((f['buys'][0][0]['event_timestamp'] or 0,asset,f))
        for wallet,positions in by_wallet.items():
            positions.sort()
            for target_time,target_asset,target_flow in positions:
                prior=[]
                for _,source_asset,source_flow in positions:
                    sells=[x for x in source_flow['sells'] if (x[0]['event_timestamp'] or 0)<=target_time]
                    if source_asset!=target_asset and sells:prior.append((sells[-1][0]['event_timestamp'] or 0,source_asset,sells[-1]))
                if not prior:continue
                sold_time,source_asset,sale=max(prior)
                latency=target_time-sold_time
                if 0<=latency<=86400:
                    first=target_flow['buys'][0]
                    migrations.append((target_asset,wallet,source_asset,sold_time,target_time,latency,str(sale[1]),str(first[1]),'observed-sequential',stamp))
        db.executemany('INSERT INTO capital_migrations VALUES(?,?,?,?,?,?,?,?,?,?)',migrations)
        set_meta(db,'capital_flow_heartbeat',stamp);set_meta(db,'capital_flow_wallet_assets',len(flows));set_meta(db,'capital_flow_clusters',len(clusters));set_meta(db,'capital_migrations',len(migrations))
    return len(flows)


def main():
    p=argparse.ArgumentParser();p.add_argument('--db',default='data/live.sqlite');p.add_argument('--interval',type=int,default=30);a=p.parse_args();db=database(a.db)
    try:
        while True:
            try:LOG.info('capital wallet-assets=%s',rebuild(db))
            except sqlite3.Error as exc:LOG.warning('capital cycle delayed: %s',exc)
            time.sleep(max(15,a.interval))
    finally:db.close()

if __name__=='__main__':logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s');main()
