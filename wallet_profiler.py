"""Derive wallet behavior and coordination clues from stored onchain evidence."""
import argparse
from collections import defaultdict
import json
import logging
import sqlite3
import time

from listener import database, now, set_meta

LOG=logging.getLogger('wallet-profiler')


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS tx_attributions(
        tx_hash TEXT PRIMARY KEY, asset TEXT NOT NULL, sender TEXT,
        event_actor TEXT, relation TEXT, checked_at TEXT NOT NULL, error TEXT);
      CREATE INDEX IF NOT EXISTS tx_attributions_asset ON tx_attributions(asset);
      CREATE TABLE IF NOT EXISTS wallet_profiles(
        wallet TEXT PRIMARY KEY, updated_at TEXT NOT NULL, buys INTEGER NOT NULL,
        sells INTEGER NOT NULL, assets INTEGER NOT NULL, early_assets INTEGER NOT NULL,
        first_block INTEGER, last_block INTEGER, smart_score REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS wallet_asset_stats(
        asset TEXT NOT NULL, wallet TEXT NOT NULL, buys INTEGER NOT NULL,
        sells INTEGER NOT NULL, first_block INTEGER, last_block INTEGER,
        early_entry INTEGER NOT NULL, PRIMARY KEY(asset,wallet));
      CREATE INDEX IF NOT EXISTS wallet_asset_asset ON wallet_asset_stats(asset);
      CREATE TABLE IF NOT EXISTS wallet_clusters(
        asset TEXT NOT NULL, cluster_key TEXT NOT NULL, kind TEXT NOT NULL,
        members INTEGER NOT NULL, transactions INTEGER NOT NULL,
        updated_at TEXT NOT NULL, PRIMARY KEY(asset,cluster_key,kind));
      CREATE TABLE IF NOT EXISTS wallet_asset_pnl(
        asset TEXT NOT NULL, wallet TEXT NOT NULL, quote_symbol TEXT,
        position_tokens REAL NOT NULL, cost_basis_quote REAL NOT NULL,
        realized_pnl_quote REAL NOT NULL, buy_quote REAL NOT NULL,
        sell_quote REAL NOT NULL, matched_sells INTEGER NOT NULL,
        coverage TEXT NOT NULL, PRIMARY KEY(asset,wallet));
      CREATE INDEX IF NOT EXISTS wallet_asset_pnl_asset ON wallet_asset_pnl(asset);
      CREATE TABLE IF NOT EXISTS wallet_performance(
        wallet TEXT PRIMARY KEY, updated_at TEXT NOT NULL, realized_assets INTEGER NOT NULL,
        wins INTEGER NOT NULL, losses INTEGER NOT NULL, win_rate REAL,
        realized_by_quote TEXT NOT NULL, coverage TEXT NOT NULL);
    ''')


def rebuild_pnl(db, limit=20000):
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'market_snapshots' not in tables:return 0
    markets={r['asset']:dict(r) for r in db.execute('SELECT asset,decimals,quote_decimals,quote_symbol FROM market_snapshots WHERE decimals IS NOT NULL AND quote_decimals IS NOT NULL')}
    rows=db.execute("SELECT asset,name,decoded FROM events WHERE asset IS NOT NULL AND name IN ('CurveBuy','CurveSell') ORDER BY block_number DESC,log_index DESC LIMIT ?",(limit,)).fetchall()[::-1]
    states=defaultdict(lambda:{'qty':0.0,'cost':0.0,'realized':0.0,'buy_quote':0.0,'sell_quote':0.0,'matched_sells':0})
    for row in rows:
        market=markets.get(row['asset'])
        if not market:continue
        values=json.loads(row['decoded']);buy=row['name']=='CurveBuy';wallet=values.get('buyer') if buy else values.get('seller')
        if not wallet:continue
        s=states[(row['asset'],wallet.lower())];td=10**market['decimals'];qd=10**market['quote_decimals']
        if buy:
            tokens=int(values.get('tokensOut') or 0)/td;quote=int(values.get('quoteIn') or 0)/qd
            s['qty']+=tokens;s['cost']+=quote;s['buy_quote']+=quote
        else:
            tokens=int(values.get('tokensIn') or 0)/td;proceeds=int(values.get('quoteOut') or 0)/qd;s['sell_quote']+=proceeds
            matched=min(s['qty'],tokens)
            if matched>0 and tokens>0:
                basis=s['cost']/s['qty']*matched if s['qty'] else 0;s['realized']+=proceeds*(matched/tokens)-basis
                s['qty']-=matched;s['cost']=max(0,s['cost']-basis);s['matched_sells']+=1
    performance=defaultdict(lambda:{'assets':0,'wins':0,'losses':0,'quotes':defaultdict(float)})
    stamp=now()
    with db:
        db.execute('DELETE FROM wallet_asset_pnl');db.execute('DELETE FROM wallet_performance')
        for (asset,wallet),s in states.items():
            quote=markets[asset]['quote_symbol'] or markets[asset].get('quote_token') or 'quote'
            db.execute('INSERT INTO wallet_asset_pnl VALUES(?,?,?,?,?,?,?,?,?,?)',(asset,wallet,quote,s['qty'],s['cost'],s['realized'],s['buy_quote'],s['sell_quote'],s['matched_sells'],'listener-window'))
            if s['matched_sells']:
                p=performance[wallet];p['assets']+=1;p['wins']+=s['realized']>0;p['losses']+=s['realized']<0;p['quotes'][quote]+=s['realized']
        for wallet,p in performance.items():
            decided=p['wins']+p['losses'];rate=round(100*p['wins']/decided,1) if decided else None
            db.execute('INSERT INTO wallet_performance VALUES(?,?,?,?,?,?,?,?)',(wallet,stamp,p['assets'],p['wins'],p['losses'],rate,json.dumps({k:round(v,8) for k,v in p['quotes'].items()}),'listener-window'))
        set_meta(db,'wallet_pnl_wallets',len(performance));set_meta(db,'wallet_pnl_heartbeat',stamp)
    return len(performance)


def rebuild(db, limit=20000, early_window=500):
    launches={r['asset']:r['created_block'] for r in db.execute('SELECT asset,MIN(created_block) created_block FROM watches WHERE asset IS NOT NULL GROUP BY asset')}
    attrs={r['tx_hash']:r for r in db.execute('SELECT tx_hash,sender,event_actor,relation FROM tx_attributions WHERE error IS NULL')}
    rows=db.execute("SELECT tx_hash,asset,name,decoded,block_number FROM events WHERE asset IS NOT NULL AND name IN ('CurveBuy','CurveSell') ORDER BY block_number DESC,log_index DESC LIMIT ?",(limit,)).fetchall()
    per=defaultdict(lambda:{'buys':0,'sells':0,'first':None,'last':None,'early':0})
    profiles=defaultdict(lambda:{'buys':0,'sells':0,'assets':set(),'early':set(),'first':None,'last':None})
    routed=defaultdict(lambda:{'members':set(),'tx':0})
    for row in rows:
        values=json.loads(row['decoded']);wallet=(values.get('buyer') if row['name']=='CurveBuy' else values.get('seller'))
        if not wallet:continue
        wallet=wallet.lower();key=(row['asset'],wallet);item=per[key];side='buys' if row['name']=='CurveBuy' else 'sells';item[side]+=1
        item['first']=row['block_number'] if item['first'] is None else min(item['first'],row['block_number']);item['last']=max(item['last'] or 0,row['block_number'])
        is_early=row['name']=='CurveBuy' and launches.get(row['asset']) is not None and row['block_number']-launches[row['asset']]<=early_window
        item['early']|=is_early;p=profiles[wallet];p[side]+=1;p['assets'].add(row['asset']);p['first']=row['block_number'] if p['first'] is None else min(p['first'],row['block_number']);p['last']=max(p['last'] or 0,row['block_number'])
        if is_early:p['early'].add(row['asset'])
        a=attrs.get(row['tx_hash'])
        if a and a['relation']=='routed' and a['sender']:
            group=routed[(row['asset'],a['sender'])];group['members'].add(wallet);group['tx']+=1
    stamp=now()
    with db:
        db.execute('DELETE FROM wallet_asset_stats');db.execute('DELETE FROM wallet_profiles');db.execute('DELETE FROM wallet_clusters')
        db.executemany('INSERT INTO wallet_asset_stats VALUES(?,?,?,?,?,?,?)',[(asset,w,v['buys'],v['sells'],v['first'],v['last'],int(v['early'])) for (asset,w),v in per.items()])
        for wallet,p in profiles.items():
            total=p['buys']+p['sells'];ratio=p['buys']/max(1,total);score=min(40,len(p['early'])*12)+min(25,len(p['assets'])*5)+min(20,ratio*20)+min(15,max(0,p['buys']-len(p['assets']))*2)
            db.execute('INSERT INTO wallet_profiles VALUES(?,?,?,?,?,?,?,?,?)',(wallet,stamp,p['buys'],p['sells'],len(p['assets']),len(p['early']),p['first'],p['last'],round(score,1)))
        for (asset,sender),g in routed.items():
            if len(g['members'])>=2:db.execute('INSERT INTO wallet_clusters VALUES(?,?,?,?,?,?)',(asset,sender,'shared-routed-sender',len(g['members']),g['tx'],stamp))
        set_meta(db,'wallet_profiler_heartbeat',stamp);set_meta(db,'wallet_profiles',len(profiles));set_meta(db,'wallet_clusters',sum(len(g['members'])>=2 for g in routed.values()))
    rebuild_pnl(db,limit)
    return len(profiles)


def main():
    p=argparse.ArgumentParser();p.add_argument('--db',default='data/live.sqlite');p.add_argument('--interval',type=int,default=30);a=p.parse_args();db=database(a.db);schema(db)
    try:
        while True:
            try:LOG.info('profiled wallets=%s',rebuild(db))
            except sqlite3.Error as exc:LOG.warning('profile cycle delayed: %s',exc)
            time.sleep(max(15,a.interval))
    finally:db.close()


if __name__=='__main__':logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s');main()
