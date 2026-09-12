"""Persistent, deduplicated alerts derived from the live radar evidence."""
import argparse
import datetime as dt
import html
import json
import logging
import os
import sqlite3
import time
from urllib import parse, request

from dashboard import read
from listener import database, now, set_meta

LOG = logging.getLogger('alerts')
RISK_ONLY_RULES={'dev-exit','contract-risk','serial-deployer','insider-exit','possible-bundled-launch','creator-clustered-supply','toxic-creator-history'}


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
      CREATE TABLE IF NOT EXISTS alert_deliveries(
        alert_id INTEGER PRIMARY KEY, channel TEXT NOT NULL, status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0, last_attempt_at TEXT, delivered_at TEXT,
        error TEXT, FOREIGN KEY(alert_id) REFERENCES alerts(id));
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
    cutoff=(dt.datetime.now(dt.timezone.utc)-dt.timedelta(hours=7)).isoformat()
    # Six hours is the final lifecycle checkpoint. Older alerts are immutable
    # calibration history and must not delay evaluation of fresh flow.
    alerts=db.execute('SELECT id,created_at,asset,evidence FROM alerts WHERE created_at>=?',(cutoff,)).fetchall();updated=0
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
        post=db.execute("SELECT name,decoded FROM events WHERE asset=? AND COALESCE(event_timestamp,0)>=? AND name IN ('CurveBuy','CurveSell','DexBuy','DexSell')",(alert['asset'],int(created.timestamp()))).fetchall() if 'events' in tables else []
        sold=set();buys=sells=0
        for row in post:
            buys+=row['name'] in ('CurveBuy','DexBuy');sells+=row['name']=='CurveSell'
            if row['name']=='CurveSell':
                wallet=(json.loads(row['decoded']).get('seller') or '').lower()
                if wallet in wallets:sold.add(wallet)
        current=_pct(price,entry);maximum=_pct(ath,entry);drawdown=round((price/ath-1)*100,2) if ath else None
        values=(alert['id'],now(),started,entry,price,ath,checkpoints['return_5m'],checkpoints['return_15m'],checkpoints['return_1h'],checkpoints['return_6h'],current,maximum,drawdown,len(wallets),len(sold),buys,sells)
        columns='alert_id,updated_at,tracking_started_at,entry_price_quote,latest_price_quote,ath_price_quote,return_5m,return_15m,return_1h,return_6h,current_return,max_return,drawdown_from_ath,source_wallets,wallets_sold,post_alert_buys,post_alert_sells'
        with db:db.execute(f'INSERT OR REPLACE INTO alert_lifecycle({columns}) VALUES('+','.join('?'*17)+')',values)
        updated+=1
    return updated


def telegram_text(alert):
    e=json.loads(alert['evidence']);symbol=html.escape(e.get('symbol') or alert['asset'][:10]);name=html.escape(e.get('name') or 'Unnamed token');asset=html.escape(alert['asset'])
    wallets=e.get('source_wallets') or []
    decimals=e.get('quote_decimals');quote=e.get('quote_symbol') or 'quote'
    def amount(raw):
        try:return f"{int(raw)/(10**int(decimals)):,.4g} {quote}" if decimals is not None else f"{raw} raw"
        except (TypeError,ValueError):return '—'
    def metric(value,prefix=''):
        if value is None:return '—'
        value=float(value)
        return prefix+(f'{value/1_000_000:.2f}m' if abs(value)>=1_000_000 else f'{value/1_000:.1f}k' if abs(value)>=1_000 else f'{value:.4g}')
    proof='\n'.join(f"• <a href=\"{w.get('wallet_url','')}\">{w['wallet'][:8]}…{w['wallet'][-6:]}</a> · buy {html.escape(amount(w.get('quote_in_raw')))} · repeat {w.get('buy_count',1)}× · win {w.get('win_rate') if w.get('win_rate') is not None else '—'}% · <a href=\"{w.get('buy_tx_url','')}\">TX</a>" for w in wallets[:5]) or '• Buyer detail belum cukup untuk diperingkat'
    mc=metric(e.get('market_cap_usd'),'$') if e.get('market_cap_usd') is not None else metric(e.get('market_cap_quote'))+' '+quote
    liq=metric(e.get('liquidity_usd'),'$') if e.get('liquidity_usd') is not None else metric(e.get('liquidity_quote'))+' '+quote
    flow=f"Repeat {e.get('ordered_repeat_wallets',0)} · size-up {e.get('increasing_size_wallets',0)} · retained {e.get('retained_wallets',0)} · qualified migration 5m {e.get('qualified_migrating_wallets_5m',0)}"
    creator=f"{e.get('creator_classification') or 'insufficient-history'} · score {e.get('creator_reputation_score') if e.get('creator_reputation_score') is not None else '—'} · launches {e.get('creator_launches') or 0} · runners/rugs {e.get('creator_runners') or 0}/{e.get('creator_rugs') or 0}"
    social=' · '.join(f'<a href="{html.escape(u)}">{label}</a>' for label,u in [('Website',e.get('social_website')),('X',e.get('social_x_url')),('Telegram',e.get('social_telegram_url')),('Discord',e.get('social_discord_url'))] if u) or 'Social identity belum ditemukan'
    distribution=f"{e.get('distribution_classification') or 'insufficient-evidence'} · bundle {e.get('bundle_score') if e.get('bundle_score') is not None else '—'} · linked buyers {e.get('creator_linked_early_buyers') or 0}"
    gmgn=f'https://gmgn.ai/robinhood/token/{alert["asset"]}'
    return (f"🔎 <b>${symbol} — {html.escape(alert['title'])}</b>\n{name}\n\n<b>CA</b> · tap untuk copy\n<code>{asset}</code>\n\n<b>MARKET</b>\nMC {mc} · Liq {liq}\nVol 1h {metric(e.get('volume_1h_quote'))} {html.escape(quote)}\n5m {metric(e.get('change_5m'))}% · 1h {metric(e.get('change_1h'))}%\nBuy/sell {e.get('buys',0)}/{e.get('sells',0)} · buyers {e.get('unique_buyers',0)}\n\n<b>FLOW</b>\n{flow}\nSafety {html.escape(e.get('safety_status','unknown'))} · score {alert['score'] or '—'}\n\n<b>CREATOR</b>\n{html.escape(creator)}\n{html.escape(distribution)}\n{social} · {html.escape(e.get('social_status') or 'no-social-evidence')}\n\n<b>BUYERS</b>\n{proof}\n\n<a href=\"{gmgn}\">📈 Open token di GMGN</a>\n<i>Onchain evidence; contract dan exit path tetap perlu diverifikasi.</i>")[:4000]


def deliver(db, token=None, chat_id=None, limit=10):
    if not token or not chat_id:return 0
    rows=db.execute("SELECT a.* FROM alert_deliveries d JOIN alerts a ON a.id=d.alert_id WHERE d.status IN ('pending','retry') AND d.attempts<5 ORDER BY a.id LIMIT ?",(limit,)).fetchall();sent=0
    for alert in rows:
        try:
            buttons={'inline_keyboard':[[{'text':'📈 Open GMGN','url':f'https://gmgn.ai/robinhood/token/{alert["asset"]}'},{'text':'🔍 Explorer','url':f'https://robinhoodchain.blockscout.com/token/{alert["asset"]}'}]]}
            body=parse.urlencode({'chat_id':chat_id,'text':telegram_text(alert),'parse_mode':'HTML','disable_web_page_preview':'true','reply_markup':json.dumps(buttons)}).encode()
            with request.urlopen(request.Request(f'https://api.telegram.org/bot{token}/sendMessage',data=body),timeout=12) as response:
                if response.status!=200:raise OSError(f'Telegram HTTP {response.status}')
            with db:db.execute("UPDATE alert_deliveries SET status='sent',attempts=attempts+1,last_attempt_at=?,delivered_at=?,error=NULL WHERE alert_id=?",(now(),now(),alert['id']))
            sent+=1
        except Exception as exc:
            with db:db.execute("UPDATE alert_deliveries SET status='retry',attempts=attempts+1,last_attempt_at=?,error=? WHERE alert_id=?",(now(),str(exc)[:300],alert['id']))
    return sent


def matches(c):
    out=[]
    identified=bool((c.get('symbol') or '').strip() or (c.get('name') or '').strip()) and c.get('market_status') not in (None,'unknown')
    market_survived=(c.get('market_observations',0)>=2 and c.get('observation_span_seconds',0)>=180
                     and (c.get('change_5m') is None or c['change_5m']>=-20)
                     and (c.get('drawdown_from_observed_high') is None or c['drawdown_from_observed_high']>=-35))
    live_flow=(c.get('buys_5m',0)>=3 and c.get('buys_5m',0)>=c.get('sells_5m',0)
               and c.get('buy_sell_ratio',0)>=1.25)
    fresh_alpha=(identified and bool(c.get('deployer')) and c.get('age_blocks') is not None and c['age_blocks']<=15000
                 and market_survived and live_flow and (c.get('last_trade_age_seconds') is None or c['last_trade_age_seconds']<=180)
                 and not c.get('dev_exit_detected') and not c.get('insider_exit_detected') and c.get('distribution_classification') not in ('possible-bundled-launch','creator-clustered-supply') and c.get('creator_classification')!='toxic-history' and (c.get('deployer_launch_count',0)<3 or c.get('creator_classification') in ('proven-runner','promising-history')) and c['safety_status']!='higher-risk')
    if c['safety_status']=='higher-risk':
        out.append(('contract-risk','critical','Contract risk terdeteksi',c.get('safety_score') or 0))
    if c.get('dev_exit_detected'):
        out.append(('dev-exit','critical','Deployer sell terdeteksi',0))
    if c.get('insider_exit_detected'):
        out.append(('insider-exit','critical','Creator-linked wallet sell terdeteksi',0))
    if c.get('distribution_classification')=='possible-bundled-launch':
        out.append(('possible-bundled-launch','critical','Possible bundled launch terdeteksi',c.get('bundle_score') or 0))
    elif c.get('distribution_classification')=='creator-clustered-supply':
        out.append(('creator-clustered-supply','high','Creator-linked supply terkonsentrasi',c.get('creator_cluster_share') or 0))
    elif fresh_alpha and c.get('distribution_classification')=='clean-early-distribution':
        out.append(('clean-early-distribution','medium','Distribusi awal terlihat bersih',c.get('activity_score') or 0))
    if identified and c.get('creator_classification')=='toxic-history':
        out.append(('toxic-creator-history','critical','Creator punya histori buruk',100-(c.get('creator_reputation_score') or 0)))
    elif identified and c.get('deployer_launch_count',0)>=3 and c.get('creator_classification') not in ('proven-runner','promising-history'):
        score=min(100,40+c['deployer_launch_count']*5)
        out.append(('serial-deployer','medium','Serial deployer belum terbukti',score))
    if fresh_alpha and c.get('creator_classification')=='proven-runner' and c.get('creator_confidence') in ('medium','high'):
        out.append(('creator-track-record','high','Creator runner kembali launch',c.get('creator_reputation_score') or 0))
    if fresh_alpha and c.get('social_cross_linked') and c.get('social_confidence')=='high':
        out.append(('social-cross-linked','medium','Website dan social identity saling terhubung',c.get('social_score') or 0))
    if fresh_alpha and c.get('profitable_wallets_30m',0)>=3 and c.get('profitable_wallets_15m',0)>=2 and c.get('independent_profitable_wallets_30m',0)>=2:
        score=min(100,55+c['profitable_wallets_5m']*8+c['profitable_wallets_15m']*5+c['independent_profitable_wallets_30m']*3)
        out.append(('smart-money-consensus','high','Profitable-wallet consensus terdeteksi',score))
    if fresh_alpha and c.get('profitable_wallets_30m',0)>=2 and c.get('smart_wallets',0)>=2 and (c.get('conviction_score') or 0)>=55 and c['safety_status']=='screened' and c['buys']>=5:
        out.append(('smart-wallet-entry','high','Beberapa early wallet masuk',c['conviction_score']))
    if fresh_alpha and c.get('ordered_repeat_wallets',0)>=2 and c.get('increasing_size_wallets',0)>=1 and c.get('retained_wallets',0)>=2:
        score=min(100,50+c['ordered_repeat_wallets']*6+c['increasing_size_wallets']*5+c.get('profitable_wallets_30m',0)*4)
        out.append(('repeat-qualified-flow','high','Repeat qualified flow terdeteksi',score))
    if fresh_alpha and c.get('qualified_migrating_wallets_5m',0)>=2 and c.get('migration_sources',0)>=1:
        score=min(100,60+c['qualified_migrating_wallets_5m']*8+c['migration_sources']*2)
        out.append(('capital-rotation','high','Rotasi modal masuk terdeteksi',score))
    if fresh_alpha and c.get('cluster_count',0)>0 and c.get('cluster_members',0)>=3 and c['buys']>=5:
        out.append(('coordinated-flow','medium','Flow terkoordinasi terdeteksi',c['activity_score']))
    if fresh_alpha and (c.get('conviction_score') or 0)>=70 and c['safety_status']=='screened' and c['unique_senders']>=3 and c['buys']>=5 and c['buy_sell_ratio']>=1.5 and c['activity_acceleration']>=1.2 and c['routed_share']<=.75:
        out.append(('trench-candidate','high','Kandidat trench terkonfirmasi',c['conviction_score']))
    elif fresh_alpha and (c.get('conviction_score') or 0)>=58 and c['safety_status']=='screened' and c['unique_senders']>=2 and c['buys']>=5 and c['activity_acceleration']>=1.5:
        out.append(('momentum-watch','medium','Momentum awal mulai terbentuk',c['conviction_score']))
    elif fresh_alpha and c['activity_score']>=55 and c['safety_status']=='screened' and c['unique_buyers']>=3 and c['buys']>=5:
        out.append(('early-watch','low','Aktivitas awal layak dipantau',c['activity_score']))
    return out


def source_wallets(db, asset, limit=5, preferred=None):
    """Capture the transactions behind a wallet alert without inventing USD/PnL."""
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'events' not in tables:return []
    profiles={r['wallet']:dict(r) for r in db.execute('SELECT * FROM wallet_profiles')} if 'wallet_profiles' in tables else {}
    capital={r['wallet']:dict(r) for r in db.execute('SELECT * FROM capital_wallet_asset WHERE asset=?',(asset,))} if 'capital_wallet_asset' in tables else {}
    performance={r['wallet']:dict(r) for r in db.execute('SELECT * FROM wallet_performance')} if 'wallet_performance' in tables else {}
    asset_pnl={r['wallet']:dict(r) for r in db.execute('SELECT * FROM wallet_asset_pnl WHERE asset=?',(asset,))} if 'wallet_asset_pnl' in tables else {}
    attrs={r['tx_hash']:dict(r) for r in db.execute('SELECT tx_hash,sender FROM tx_attributions WHERE error IS NULL')} if 'tx_attributions' in tables else {}
    rows=db.execute("SELECT tx_hash,block_number,observed_at,event_timestamp,name,decoded FROM events WHERE asset=? AND name IN ('CurveBuy','CurveSell','DexBuy','DexSell') ORDER BY block_number,log_index",(asset,)).fetchall()
    activity=[]
    for row in rows:
        values=json.loads(row['decoded']);a=attrs.get(row['tx_hash'])
        wallet=(a.get('sender') if row['name'].startswith('Dex') and a else None) or (values.get('buyer') if row['name'].endswith('Buy') else values.get('seller'))
        if wallet:activity.append((row,wallet.lower(),values))
    latest={}
    for row,wallet,values in activity:
        if row['name'] in ('CurveBuy','DexBuy'):latest[wallet]=(row,values)
    preferred=set(preferred or ())
    eligible=[item for item in latest.items() if profiles.get(item[0],{}).get('smart_score',0)>=55 or capital.get(item[0],{}).get('buy_count',0)>=2]
    ranked=sorted(eligible,key=lambda item:(item[0] in preferred,capital.get(item[0],{}).get('buy_count',0),profiles.get(item[0],{}).get('smart_score',0),item[1][0]['block_number']),reverse=True)[:limit]
    result=[]
    for wallet,(buy,values) in ranked:
        sells=[(r,v) for r,w,v in activity if w==wallet and r['name'] in ('CurveSell','DexSell') and r['block_number']>=buy['block_number']]
        p=profiles.get(wallet,{});cap=capital.get(wallet,{})
        perf=performance.get(wallet,{});ap=asset_pnl.get(wallet,{})
        result.append({'wallet':wallet,'buy_tx':buy['tx_hash'],'buy_block':buy['block_number'],'buy_time':buy['event_timestamp'] or buy['observed_at'],
          'quote_in_raw':values.get('quoteIn'),'tokens_out_raw':values.get('tokensOut'),'recorded_sells_since_buy':len(sells),
          'quote_out_raw_since_buy':str(sum(int(v.get('quoteOut') or 0) for _,v in sells)),'smart_score':p.get('smart_score'),
          'tracked_assets':p.get('assets',0),'early_assets':p.get('early_assets',0),'tracked_buys':p.get('buys',0),'tracked_sells':p.get('sells',0),
          'buy_count':cap.get('buy_count',1),'size_trend':cap.get('size_trend'),'retained_raw':cap.get('retained_raw'),
          'wallet_url':'https://robinhoodchain.blockscout.com/address/'+wallet,'buy_tx_url':'https://robinhoodchain.blockscout.com/tx/'+buy['tx_hash'],
          'win_rate':perf.get('win_rate'),'realized_assets':perf.get('realized_assets',0),'realized_by_quote':json.loads(perf.get('realized_by_quote','{}')),
          'asset_realized_pnl_quote':ap.get('realized_pnl_quote'),'asset_quote_symbol':ap.get('quote_symbol'),
          'pnl_coverage':perf.get('coverage') or ap.get('coverage'),'realized_pnl_usd':None})
    return result


def evidence(c, db=None):
    keys=('protocol','activity_score','conviction_score','safety_score','safety_status','buys','sells','buys_5m','sells_5m','last_trade_age_seconds','dev_buy_count','dev_sell_count','dev_exit_detected','deployer','creator_attribution','creator_confidence','insider_wallets','insider_sell_count','insider_exit_detected','creator_cluster_share','early_recipients','early_buyers','creator_linked_early_buyers','same_block_buyers','similar_size_buyers','shared_funding_clusters','bundle_score','distribution_classification','creator_launches','creator_indexed_assets','creator_survivors','creator_runners','creator_rugs','creator_runner_rate','creator_rug_rate','creator_median_peak_multiple','creator_reputation_score','creator_classification','creator_confidence','social_website','social_x_url','social_telegram_url','social_discord_url','social_source_count','social_cross_linked','social_score','social_confidence','social_status','deployer_launch_count','deployer_other_assets','unique_buyers','repeat_buyers','unique_senders','routed_share','smart_wallets','best_wallet_score','cluster_count','cluster_members','ordered_repeat_wallets','increasing_size_wallets','retained_wallets','provisional_funding_roots','shared_sender_wallets','shared_sender_clusters','migrating_wallets','migration_sources','fastest_migration_seconds','qualified_migrating_wallets_5m','activity_acceleration','age_blocks','buy_sell_ratio','market_observations','observation_span_seconds','drawdown_from_observed_high','safety_findings','symbol','name','quote_symbol','quote_decimals','price_quote','price_usd','market_cap_quote','market_cap_usd','liquidity_quote','liquidity_usd','volume_5m_quote','volume_1h_quote','volume_24h_quote','change_5m','change_1h','change_6h','change_24h','market_source','market_status','profitable_wallets_5m','profitable_wallets_15m','profitable_wallets_30m','independent_profitable_wallets_30m','unattributed_profitable_wallets_30m','consensus_proof')
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
            prior=db.execute('SELECT 1 FROM alerts WHERE asset=? AND rule=? LIMIT 1',key).fetchone()
            old=db.execute('SELECT active,last_score,last_alert_at FROM alert_states WHERE asset=? AND rule=?',key).fetchone()
            last_epoch=0
            if old and old['last_alert_at']:
                try: last_epoch=__import__('datetime').datetime.fromisoformat(old['last_alert_at']).timestamp()
                except ValueError: pass
            should=not prior
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


def telegram_worthy(db, alert_id):
    """Telegram is the actionable alpha feed; risk evidence stays on dashboard."""
    alert=db.execute('SELECT asset,rule FROM alerts WHERE id=?',(alert_id,)).fetchone()
    if not alert:return False
    return alert['rule'] not in RISK_ONLY_RULES


def suppress_untracked_risk_deliveries(db):
    placeholders=','.join('?' for _ in RISK_ONLY_RULES)
    rows=db.execute(f"SELECT d.alert_id FROM alert_deliveries d JOIN alerts a ON a.id=d.alert_id WHERE d.status!='sent' AND a.rule IN ({placeholders})",tuple(RISK_ONLY_RULES)).fetchall()
    suppressed=0
    with db:
        for row in rows:
            if not telegram_worthy(db,row['alert_id']):
                db.execute("UPDATE alert_deliveries SET status='suppressed',error='risk event for asset without prior positive alert' WHERE alert_id=?",(row['alert_id'],))
                suppressed+=1
    return suppressed


def cycle(path, cooldown=1800, improvement=8):
    snapshot=read(path)
    db=database(path);schema(db)
    try:
        # Record liveness before optional enrichment/evaluation work so an
        # expensive candidate cannot make the worker appear dead.
        with db:set_meta(db,'alert_heartbeat',now())
        health=snapshot['health']
        # Free WSS endpoints can reconnect between otherwise current heads. The
        # candidate-level live-flow gate still rejects trades older than 180s.
        if health.get('state') not in ('healthy','degraded','recovering-history') \
           or (health.get('age_seconds') or 10**9)>600 \
           or (health.get('lag_blocks') or 0)>100:
            return []
        tracked=track_lifecycle(db)
        emitted=evaluate(db,snapshot['candidates'],cooldown,improvement)
        with db:
            for alert_id in emitted:
                status='pending' if telegram_worthy(db,alert_id) else 'suppressed'
                db.execute("INSERT OR IGNORE INTO alert_deliveries(alert_id,channel,status,error) VALUES(?,'telegram',?,?)",(alert_id,status,None if status=='pending' else 'risk event for asset without prior positive alert'))
        suppress_untracked_risk_deliveries(db)
        delivered=deliver(db,os.environ.get('TELEGRAM_BOT_TOKEN'),os.environ.get('TELEGRAM_CHAT_ID'))
        if emitted:tracked=track_lifecycle(db)
        with db:
            set_meta(db,'alert_heartbeat',now())
            set_meta(db,'alert_active_rules',db.execute('SELECT COUNT(*) FROM alert_states WHERE active=1').fetchone()[0])
            set_meta(db,'alert_last_emitted',len(emitted))
            set_meta(db,'alert_lifecycles',tracked)
            set_meta(db,'telegram_configured','1' if os.environ.get('TELEGRAM_BOT_TOKEN') and os.environ.get('TELEGRAM_CHAT_ID') else '0')
            set_meta(db,'telegram_last_delivered',delivered)
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
