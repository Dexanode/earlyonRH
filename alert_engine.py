"""Persistent, deduplicated alerts derived from the live radar evidence."""
import argparse
import datetime as dt
import json
import logging
import sqlite3
import time

from dashboard import read
from listener import database, now, set_meta

LOG = logging.getLogger('alerts')


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS alerts(
        id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
        asset TEXT NOT NULL, rule TEXT NOT NULL, severity TEXT NOT NULL,
        title TEXT NOT NULL, score REAL, evidence TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS alerts_created ON alerts(created_at DESC);
      CREATE TABLE IF NOT EXISTS alert_states(
        asset TEXT NOT NULL, rule TEXT NOT NULL, active INTEGER NOT NULL,
        last_score REAL, last_alert_at TEXT, PRIMARY KEY(asset,rule));
      CREATE TABLE IF NOT EXISTS alert_lifecycle(
        alert_id INTEGER PRIMARY KEY, updated_at TEXT NOT NULL, tracking_started_at TEXT NOT NULL,
        entry_price_quote REAL, latest_price_quote REAL, ath_price_quote REAL,
        return_5m REAL, return_15m REAL, return_1h REAL, return_6h REAL,
        current_return REAL, max_return REAL, drawdown_from_ath REAL,
        source_wallets INTEGER NOT NULL DEFAULT 0, wallets_sold INTEGER NOT NULL DEFAULT 0,
        post_alert_buys INTEGER NOT NULL DEFAULT 0, post_alert_sells INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(alert_id) REFERENCES alerts(id));
    ''')
    columns={r[1] for r in db.execute('PRAGMA table_info(alert_lifecycle)')}
    if 'tracking_started_at' not in columns:
        with db:
            db.execute('ALTER TABLE alert_lifecycle ADD COLUMN tracking_started_at TEXT')
            db.execute('UPDATE alert_lifecycle SET tracking_started_at=updated_at WHERE tracking_started_at IS NULL')


def _pct(price, entry):
    return round((price / entry - 1) * 100, 2) if price and entry else None


def track_lifecycle(db):
    """Update every alert against locally observed market and wallet activity."""
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'market_snapshots' not in tables:return 0
    alerts=db.execute('SELECT id,created_at,asset,evidence FROM alerts').fetchall();updated=0
    for alert in alerts:
        market=db.execute('SELECT price_quote FROM market_snapshots WHERE asset=?',(alert['asset'],)).fetchone()
        if not market or not market['price_quote']:continue
        price=float(market['price_quote']);old=db.execute('SELECT * FROM alert_lifecycle WHERE alert_id=?',(alert['id'],)).fetchone()
        evidence=json.loads(alert['evidence']);wallets={w['wallet'].lower() for w in evidence.get('source_wallets',[]) if w.get('wallet')}
        created=dt.datetime.fromisoformat(alert['created_at']);started=(old['tracking_started_at'] if old else now());elapsed=max(0,(dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(started)).total_seconds())
        entry=float(old['entry_price_quote']) if old and old['entry_price_quote'] else price
        ath=max(price,float(old['ath_price_quote'] or price)) if old else price
        checkpoints={k:(old[k] if old else None) for k in ('return_5m','return_15m','return_1h','return_6h')}
        for key,seconds in (('return_5m',300),('return_15m',900),('return_1h',3600),('return_6h',21600)):
            if checkpoints[key] is None and elapsed>=seconds:checkpoints[key]=_pct(price,entry)
        post=db.execute("SELECT name,decoded FROM events WHERE asset=? AND COALESCE(event_timestamp,0)>=? AND name IN ('CurveBuy','CurveSell')",(alert['asset'],int(created.timestamp()))).fetchall() if 'events' in tables else []
        sold=set();buys=sells=0
        for row in post:
            buys+=row['name']=='CurveBuy';sells+=row['name']=='CurveSell'
            if row['name']=='CurveSell':
                wallet=(json.loads(row['decoded']).get('seller') or '').lower()
                if wallet in wallets:sold.add(wallet)
        current=_pct(price,entry);maximum=_pct(ath,entry);drawdown=round((price/ath-1)*100,2) if ath else None
        values=(alert['id'],now(),started,entry,price,ath,checkpoints['return_5m'],checkpoints['return_15m'],checkpoints['return_1h'],checkpoints['return_6h'],current,maximum,drawdown,len(wallets),len(sold),buys,sells)
        with db:db.execute('INSERT OR REPLACE INTO alert_lifecycle VALUES('+','.join('?'*17)+')',values)
        updated+=1
    return updated


def matches(c):
    out=[]
    if c['safety_status']=='higher-risk':
        out.append(('contract-risk','critical','Contract risk terdeteksi',c.get('safety_score') or 0))
    if c.get('profitable_wallets_30m',0)>=3 and c.get('profitable_wallets_15m',0)>=2 and c.get('independent_profitable_wallets_30m',0)>=2 and c['safety_status']!='higher-risk':
        score=min(100,55+c['profitable_wallets_5m']*8+c['profitable_wallets_15m']*5+c['independent_profitable_wallets_30m']*3)
        out.append(('smart-money-consensus','high','Profitable-wallet consensus terdeteksi',score))
    if c.get('smart_wallets',0)>=2 and (c.get('conviction_score') or 0)>=55 and c['safety_status']=='screened' and c['buys']>=5:
        out.append(('smart-wallet-entry','high','Beberapa early wallet masuk',c['conviction_score']))
    elif c.get('smart_wallets',0)>=5 and c['activity_score']>=60 and c['buys']>=20 and c['sells']>=5 and (c.get('age_blocks') is None or c['age_blocks']<=3000):
        out.append(('smart-wallet-watch','medium','Smart-wallet flow perlu diperiksa',c['activity_score']))
    if c.get('cluster_count',0)>0 and c.get('cluster_members',0)>=3 and c['buys']>=5:
        out.append(('coordinated-flow','medium','Flow terkoordinasi terdeteksi',c['activity_score']))
    if (c.get('conviction_score') or 0)>=70 and c['safety_status']=='screened' and c['unique_senders']>=3 and c['buys']>=5 and c['buy_sell_ratio']>=1.5 and c['activity_acceleration']>=1.2 and c['routed_share']<=.75:
        out.append(('trench-candidate','high','Kandidat trench terkonfirmasi',c['conviction_score']))
    elif (c.get('conviction_score') or 0)>=58 and c['safety_status']=='screened' and c['unique_senders']>=2 and c['buys']>=5 and c['activity_acceleration']>=1.5:
        out.append(('momentum-watch','medium','Momentum awal mulai terbentuk',c['conviction_score']))
    elif c['activity_score']>=55 and c['safety_status']=='screened' and c['unique_buyers']>=3 and c['buys']>=5 and (c.get('age_blocks') is None or c['age_blocks']<=3000):
        out.append(('early-watch','low','Aktivitas awal layak dipantau',c['activity_score']))
    return out


def source_wallets(db, asset, limit=5, preferred=None):
    """Capture the transactions behind a wallet alert without inventing USD/PnL."""
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {'wallet_profiles','events'}.issubset(tables): return []
    profiles={r['wallet']:dict(r) for r in db.execute('SELECT * FROM wallet_profiles WHERE smart_score>=55')}
    performance={r['wallet']:dict(r) for r in db.execute('SELECT * FROM wallet_performance')} if 'wallet_performance' in tables else {}
    asset_pnl={r['wallet']:dict(r) for r in db.execute('SELECT * FROM wallet_asset_pnl WHERE asset=?',(asset,))} if 'wallet_asset_pnl' in tables else {}
    rows=db.execute("SELECT tx_hash,block_number,observed_at,event_timestamp,name,decoded FROM events WHERE asset=? AND name IN ('CurveBuy','CurveSell') ORDER BY block_number,log_index",(asset,)).fetchall()
    activity=[]
    for row in rows:
        values=json.loads(row['decoded']);wallet=(values.get('buyer') if row['name']=='CurveBuy' else values.get('seller'))
        if wallet and wallet.lower() in profiles:activity.append((row,wallet.lower(),values))
    latest={}
    for row,wallet,values in activity:
        if row['name']=='CurveBuy':latest[wallet]=(row,values)
    preferred=set(preferred or ())
    ranked=sorted(latest.items(),key=lambda item:(item[0] in preferred,profiles[item[0]]['smart_score'],item[1][0]['block_number']),reverse=True)[:limit]
    result=[]
    for wallet,(buy,values) in ranked:
        sells=[(r,v) for r,w,v in activity if w==wallet and r['name']=='CurveSell' and r['block_number']>=buy['block_number']]
        p=profiles[wallet]
        perf=performance.get(wallet,{});ap=asset_pnl.get(wallet,{})
        result.append({'wallet':wallet,'buy_tx':buy['tx_hash'],'buy_block':buy['block_number'],'buy_time':buy['event_timestamp'] or buy['observed_at'],
          'quote_in_raw':values.get('quoteIn'),'tokens_out_raw':values.get('tokensOut'),'recorded_sells_since_buy':len(sells),
          'quote_out_raw_since_buy':str(sum(int(v.get('quoteOut') or 0) for _,v in sells)),'smart_score':p['smart_score'],
          'tracked_assets':p['assets'],'early_assets':p['early_assets'],'tracked_buys':p['buys'],'tracked_sells':p['sells'],
          'wallet_url':'https://robinhoodchain.blockscout.com/address/'+wallet,'buy_tx_url':'https://robinhoodchain.blockscout.com/tx/'+buy['tx_hash'],
          'win_rate':perf.get('win_rate'),'realized_assets':perf.get('realized_assets',0),'realized_by_quote':json.loads(perf.get('realized_by_quote','{}')),
          'asset_realized_pnl_quote':ap.get('realized_pnl_quote'),'asset_quote_symbol':ap.get('quote_symbol'),
          'pnl_coverage':perf.get('coverage') or ap.get('coverage'),'realized_pnl_usd':None})
    return result


def evidence(c, db=None):
    keys=('protocol','activity_score','conviction_score','safety_score','safety_status','buys','sells','unique_buyers','repeat_buyers','unique_senders','routed_share','smart_wallets','best_wallet_score','cluster_count','cluster_members','activity_acceleration','age_blocks','buy_sell_ratio','safety_findings','symbol','name','quote_symbol','price_quote','price_usd','market_cap_quote','market_cap_usd','liquidity_quote','liquidity_usd','volume_5m_quote','volume_1h_quote','volume_24h_quote','change_5m','change_1h','change_6h','change_24h','market_source','market_status','profitable_wallets_5m','profitable_wallets_15m','profitable_wallets_30m','independent_profitable_wallets_30m','unattributed_profitable_wallets_30m','consensus_proof')
    out={k:c.get(k) for k in keys}
    preferred=[p['wallet'] for p in c.get('consensus_proof',[])]
    wallets=source_wallets(db,c['id'],preferred=preferred) if db else []
    out.update(source_wallets=wallets,market_data_status=c.get('market_status') or 'unknown',wallet_pnl_status='listener-window' if any(w.get('pnl_coverage') for w in wallets) else 'unknown',
               minting_capability='unknown',contract_source_verification='unknown')
    return out


def evaluate(db, candidates, cooldown=1800, improvement=8):
    emitted=[]; seen=set(); epoch=time.time()
    for c in candidates:
        for rule,severity,title,score in matches(c):
            key=(c['id'],rule);seen.add(key)
            old=db.execute('SELECT active,last_score,last_alert_at FROM alert_states WHERE asset=? AND rule=?',key).fetchone()
            last_epoch=0
            if old and old['last_alert_at']:
                try: last_epoch=__import__('datetime').datetime.fromisoformat(old['last_alert_at']).timestamp()
                except ValueError: pass
            should=not old or not old['active'] or (epoch-last_epoch>=cooldown and score-(old['last_score'] or 0)>=improvement)
            stamp=now()
            with db:
                if should:
                    payload=evidence(c,db)
                    cur=db.execute('INSERT INTO alerts(created_at,asset,rule,severity,title,score,evidence) VALUES(?,?,?,?,?,?,?)',(stamp,c['id'],rule,severity,title,score,json.dumps(payload)))
                    emitted.append(cur.lastrowid)
                db.execute('INSERT INTO alert_states VALUES(?,?,?,?,?) ON CONFLICT(asset,rule) DO UPDATE SET active=1,last_score=excluded.last_score,last_alert_at=COALESCE(excluded.last_alert_at,alert_states.last_alert_at)',(c['id'],rule,1,score,stamp if should else None))
    active=db.execute('SELECT asset,rule FROM alert_states WHERE active=1').fetchall()
    with db:
        for row in active:
            if (row['asset'],row['rule']) not in seen:
                db.execute('UPDATE alert_states SET active=0 WHERE asset=? AND rule=?',(row['asset'],row['rule']))
    return emitted


def cycle(path, cooldown=1800, improvement=8):
    snapshot=read(path)
    db=database(path);schema(db)
    try:
        tracked=track_lifecycle(db)
        if snapshot['health'].get('state')!='healthy': return []
        emitted=evaluate(db,snapshot['candidates'],cooldown,improvement)
        if emitted:tracked=track_lifecycle(db)
        with db:
            set_meta(db,'alert_heartbeat',now())
            set_meta(db,'alert_active_rules',db.execute('SELECT COUNT(*) FROM alert_states WHERE active=1').fetchone()[0])
            set_meta(db,'alert_last_emitted',len(emitted))
            set_meta(db,'alert_lifecycles',tracked)
        return emitted
    finally:db.close()


def main():
    p=argparse.ArgumentParser();p.add_argument('--db',default='data/live.sqlite');p.add_argument('--interval',type=int,default=15);p.add_argument('--cooldown',type=int,default=1800);a=p.parse_args()
    while True:
        try:
            ids=cycle(a.db,a.cooldown)
            if ids: LOG.info('emitted %s alerts',len(ids))
        except sqlite3.Error as exc: LOG.warning('alert cycle delayed: %s',exc)
        time.sleep(max(5,a.interval))


if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s');main()
