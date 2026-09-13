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
from listener import RPC, database, now, set_meta
from market_normalizer import metadata

LOG = logging.getLogger('alerts')
RISK_ONLY_RULES={'dev-exit','contract-risk','serial-deployer','insider-exit','possible-bundled-launch','creator-clustered-supply','toxic-creator-history'}
TELEGRAM_ALPHA_RULES={'established-smart-money','smart-money-consensus','creator-track-record','capital-rotation'}
NATIVE_USD_CACHE={'value':None,'at':0.0}


def native_usd():
    """Cached keyless ETH/USD quote used only for human-readable alert values."""
    if NATIVE_USD_CACHE['value'] and time.time()-NATIVE_USD_CACHE['at']<60:return NATIVE_USD_CACHE['value']
    try:
        req=request.Request('https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd',headers={'Accept':'application/json','User-Agent':'earlyonRH/1'})
        with request.urlopen(req,timeout=8) as response:value=float(json.load(response)['ethereum']['usd'])
        NATIVE_USD_CACHE.update(value=value,at=time.time());return value
    except (OSError,KeyError,TypeError,ValueError):return NATIVE_USD_CACHE['value']


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
        discovery_entry_price_quote REAL, discovery_ath_price_quote REAL,
        discovery_current_return REAL, discovery_max_return REAL,
        source_wallets INTEGER NOT NULL DEFAULT 0, wallets_sold INTEGER NOT NULL DEFAULT 0,
        post_alert_buys INTEGER NOT NULL DEFAULT 0, post_alert_sells INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(alert_id) REFERENCES alerts(id));
      CREATE TABLE IF NOT EXISTS alert_deliveries(
        alert_id INTEGER PRIMARY KEY, channel TEXT NOT NULL, status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0, last_attempt_at TEXT, delivered_at TEXT,
        error TEXT, FOREIGN KEY(alert_id) REFERENCES alerts(id));
      CREATE TABLE IF NOT EXISTS alert_milestones(
        id INTEGER PRIMARY KEY AUTOINCREMENT, alert_id INTEGER NOT NULL,
        asset TEXT NOT NULL, multiple INTEGER NOT NULL, reached_at TEXT NOT NULL,
        peak_return REAL NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
        delivered_at TEXT, error TEXT, confirmed_samples INTEGER NOT NULL DEFAULT 0,
        confirmation_span_seconds INTEGER NOT NULL DEFAULT 0,
        UNIQUE(asset,multiple));
    ''')
    columns={r[1] for r in db.execute('PRAGMA table_info(alert_lifecycle)')}
    if 'tracking_started_at' not in columns:
        with db:
            db.execute('ALTER TABLE alert_lifecycle ADD COLUMN tracking_started_at TEXT')
            db.execute('UPDATE alert_lifecycle SET tracking_started_at=updated_at WHERE tracking_started_at IS NULL')
    for column in ('discovery_entry_price_quote','discovery_ath_price_quote','discovery_current_return','discovery_max_return'):
        if column not in columns:
            with db:db.execute(f'ALTER TABLE alert_lifecycle ADD COLUMN {column} REAL')
    milestone_columns={r[1] for r in db.execute('PRAGMA table_info(alert_milestones)')}
    for column,definition in (('confirmed_samples','INTEGER NOT NULL DEFAULT 0'),('confirmation_span_seconds','INTEGER NOT NULL DEFAULT 0')):
        if column not in milestone_columns:
            with db:db.execute(f'ALTER TABLE alert_milestones ADD COLUMN {column} {definition}')
    migrated=db.execute("SELECT value FROM meta WHERE key='milestone_confirmation_v2'").fetchone()
    if not migrated:
        with db:
            db.execute("UPDATE alert_milestones SET status='legacy-unverified',error='created before three-observation confirmation' WHERE status IN ('pending','retry','sent')")
            set_meta(db,'milestone_confirmation_v2','1')
    cleaned=db.execute("SELECT value FROM meta WHERE key='milestone_confirmation_v3'").fetchone()
    if not cleaned:
        with db:
            db.execute("UPDATE alert_milestones SET status='legacy-unverified',error='insufficient confirmation samples' WHERE confirmed_samples<3 OR confirmation_span_seconds<30")
            set_meta(db,'milestone_confirmation_v3','1')
    live_since=db.execute("SELECT value FROM meta WHERE key='milestone_live_since_v4'").fetchone()
    if not live_since:
        activated=now()
        last_alert_id=db.execute('SELECT COALESCE(MAX(id),0) FROM alerts').fetchone()[0]
        with db:
            # A code deploy must never turn historical observations into fresh
            # Telegram notifications. Only alerts created after activation can
            # produce milestones under this version.
            db.execute("UPDATE alert_milestones SET status='legacy-unverified',error='predates live milestone activation' WHERE status IN ('pending','retry')")
            set_meta(db,'milestone_live_since_v4',activated)
            set_meta(db,'milestone_min_alert_id_v4',last_alert_id)


def _pct(price, entry):
    return round((price / entry - 1) * 100, 2) if price and entry else None


def track_lifecycle(db):
    """Update every alert against locally observed market and wallet activity."""
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'market_snapshots' not in tables:return 0
    cutoff=(dt.datetime.now(dt.timezone.utc)-dt.timedelta(hours=7)).isoformat()
    # Six hours is the final lifecycle checkpoint. Older alerts are immutable
    # calibration history and must not delay evaluation of fresh flow.
    min_alert_row=db.execute("SELECT value FROM meta WHERE key='milestone_min_alert_id_v4'").fetchone()
    milestone_min_alert_id=int(min_alert_row[0]) if min_alert_row else 0
    alerts=db.execute('SELECT id,created_at,asset,evidence FROM alerts WHERE created_at>=?',(cutoff,)).fetchall();updated=0
    for alert in alerts:
        market=db.execute('SELECT price_quote,decimals,quote_decimals FROM market_snapshots WHERE asset=?',(alert['asset'],)).fetchone()
        if not market or not market['price_quote']:continue
        price=float(market['price_quote']);old=db.execute('SELECT * FROM alert_lifecycle WHERE alert_id=?',(alert['id'],)).fetchone()
        evidence=json.loads(alert['evidence']);wallets={w['wallet'].lower() for w in evidence.get('source_wallets',[]) if w.get('wallet')}
        created=dt.datetime.fromisoformat(alert['created_at']);started=(old['tracking_started_at'] if old else now());elapsed=max(0,(dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(started)).total_seconds())
        trade_points=[]
        if market['decimals'] is not None and market['quote_decimals'] is not None:
            for trade in db.execute("SELECT event_timestamp,decoded FROM events WHERE asset=? AND name IN ('CurveBuy','CurveSell','DexBuy','DexSell') ORDER BY block_number,log_index",(alert['asset'],)):
                decoded=json.loads(trade['decoded']);quote_raw=int(decoded.get('quoteIn') or decoded.get('quoteOut') or 0);token_raw=int(decoded.get('tokensOut') or decoded.get('tokensIn') or 0)
                if quote_raw and token_raw:trade_points.append((trade['event_timestamp'] or 0,(quote_raw/(10**market['quote_decimals']))/(token_raw/(10**market['decimals']))))
        alert_epoch=int(created.timestamp());at_alert=[p for stamp,p in trade_points if stamp<=alert_epoch]
        entry=at_alert[-1] if at_alert else (trade_points[0][1] if trade_points else (float(old['entry_price_quote']) if old and old['entry_price_quote'] else price))
        observations=[]
        if 'market_observations' in tables:
            observations=[(row['observed_at'],float(row['price_quote'])) for row in db.execute(
                "SELECT observed_at,price_quote FROM market_observations WHERE asset=? AND observed_at>=? AND price_quote>0 ORDER BY observed_at",
                (alert['asset'],alert['created_at']))]
        # Confirmed ATH is the third-highest independent market observation.
        # A lone indexer spike, migration fill, or current snapshot cannot create it.
        confirmed_prices=sorted(point[1] for point in observations)
        confirmed_peak=confirmed_prices[-3] if len(confirmed_prices)>=3 else (float(old['ath_price_quote']) if old and old['ath_price_quote'] else entry)
        ath=max(entry,confirmed_peak)
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
        discovery_entry=entry;discovery_ath=ath;discovery_current=current;discovery_maximum=maximum
        values=(alert['id'],now(),started,entry,price,ath,checkpoints['return_5m'],checkpoints['return_15m'],checkpoints['return_1h'],checkpoints['return_6h'],current,maximum,drawdown,discovery_entry,discovery_ath,discovery_current,discovery_maximum,len(wallets),len(sold),buys,sells)
        columns='alert_id,updated_at,tracking_started_at,entry_price_quote,latest_price_quote,ath_price_quote,return_5m,return_15m,return_1h,return_6h,current_return,max_return,drawdown_from_ath,discovery_entry_price_quote,discovery_ath_price_quote,discovery_current_return,discovery_max_return,source_wallets,wallets_sold,post_alert_buys,post_alert_sells'
        with db:db.execute(f'INSERT OR REPLACE INTO alert_lifecycle({columns}) VALUES('+','.join('?'*21)+')',values)
        # Existing alerts remain useful calibration history, but cannot create
        # retroactive notifications after a deploy or migration.
        if alert['id'] <= milestone_min_alert_id:
            continue
        for multiple in (2,3,5,10):
            qualifying=[stamp for stamp,value in observations if value>=entry*multiple]
            if len(qualifying)>=3:
                first=dt.datetime.fromisoformat(qualifying[0]);last=dt.datetime.fromisoformat(qualifying[-1]);span=max(0,int((last-first).total_seconds()))
                if span>=30:
                    with db:db.execute('''INSERT INTO alert_milestones(alert_id,asset,multiple,reached_at,peak_return,confirmed_samples,confirmation_span_seconds)
                      VALUES(?,?,?,?,?,?,?) ON CONFLICT(asset,multiple) DO UPDATE SET
                      alert_id=excluded.alert_id,reached_at=excluded.reached_at,peak_return=excluded.peak_return,
                      confirmed_samples=excluded.confirmed_samples,confirmation_span_seconds=excluded.confirmation_span_seconds,
                      status=CASE WHEN alert_milestones.status='legacy-unverified' THEN 'pending' ELSE alert_milestones.status END,
                      error=CASE WHEN alert_milestones.status='legacy-unverified' THEN NULL ELSE alert_milestones.error END''',
                      (alert['id'],alert['asset'],multiple,qualifying[0],maximum,len(qualifying),span))
        updated+=1
    return updated


def telegram_text(alert):
    e=json.loads(alert['evidence']);raw_asset=alert['asset'];symbol=html.escape(e.get('symbol') or f'NEW-{raw_asset[2:8].upper()}');name=html.escape(e.get('name') or 'Metadata pending · launchpad token');asset=html.escape(raw_asset)
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
    fx=native_usd() if quote.upper() in ('ETH','WETH') and (e.get('market_cap_quote') is not None or e.get('liquidity_quote') is not None) else None
    mc_usd=e.get('market_cap_usd') if e.get('market_cap_usd') is not None else e.get('market_cap_quote')*fx if e.get('market_cap_quote') is not None and fx else None
    liq_usd=e.get('liquidity_usd') if e.get('liquidity_usd') is not None else e.get('liquidity_quote')*fx if e.get('liquidity_quote') is not None and fx else None
    mc=metric(mc_usd,'$') if mc_usd is not None else metric(e.get('market_cap_quote'))+' '+quote
    liq=metric(liq_usd,'$') if liq_usd is not None else metric(e.get('liquidity_quote'))+' '+quote
    progress=f" · curve {e.get('launch_curve_progress_pct')}%" if e.get('launch_curve_progress_pct') is not None else ''
    lifecycle=f"{e.get('launch_protocol') or e.get('protocol') or 'unknown'} · {e.get('launch_stage') or 'observed'}{progress} · GMGN holders {e.get('gmgn_holder_count') if e.get('gmgn_holder_count') is not None else '—'}"
    flow=f"Repeat {e.get('ordered_repeat_wallets',0)} · size-up {e.get('increasing_size_wallets',0)} · retained {e.get('retained_wallets',0)} · qualified migration 5m {e.get('qualified_migrating_wallets_5m',0)}"
    creator=f"{e.get('creator_classification') or 'insufficient-history'} · score {e.get('creator_reputation_score') if e.get('creator_reputation_score') is not None else '—'} · launches {e.get('creator_launches') or 0} · runners/rugs {e.get('creator_runners') or 0}/{e.get('creator_rugs') or 0}"
    social=' · '.join(f'<a href="{html.escape(u)}">{label}</a>' for label,u in [('Website',e.get('social_website')),('X',e.get('social_x_url')),('Telegram',e.get('social_telegram_url')),('Discord',e.get('social_discord_url'))] if u) or 'Social identity belum ditemukan'
    distribution=f"{e.get('distribution_classification') or 'insufficient-evidence'} · bundle {e.get('bundle_score') if e.get('bundle_score') is not None else '—'} · linked buyers {e.get('creator_linked_early_buyers') or 0}"
    gmgn=f'https://gmgn.ai/robinhood/token/{alert["asset"]}'
    return (f"🔎 <b>${symbol} — {html.escape(alert['title'])}</b>\n{name}\n\n<b>CA</b> · tap untuk copy\n<code>{asset}</code>\n\n<b>LIFECYCLE</b>\n{html.escape(lifecycle)}\n\n<b>MARKET</b>\nMC {mc} · Liq {liq}\nVol 1h {metric(e.get('volume_1h_quote'))} {html.escape(quote)}\n5m {metric(e.get('change_5m'))}% · 1h {metric(e.get('change_1h'))}%\nBuy/sell {e.get('buys',0)}/{e.get('sells',0)} · buyers {e.get('unique_buyers',0)}\n\n<b>FLOW</b>\n{flow}\nSafety {html.escape(e.get('safety_status','unknown'))} · score {alert['score'] or '—'}\n\n<b>CREATOR</b>\n{html.escape(creator)}\n{html.escape(distribution)}\n{social} · {html.escape(e.get('social_status') or 'no-social-evidence')}\n\n<b>BUYERS</b>\n{proof}\n\n<a href=\"{gmgn}\">📈 Open token di GMGN</a>\n<i>Onchain evidence; contract dan exit path tetap perlu diverifikasi.</i>")[:4000]


def hydrate_metadata(db, alert):
    """Refresh alert evidence with identity and market data found after detection."""
    alert=dict(alert);e=json.loads(alert['evidence'])
    try:
        row=db.execute('SELECT symbol,name FROM token_metadata WHERE address=? AND error IS NULL',(alert['asset'],)).fetchone()
    except sqlite3.OperationalError:
        row=None
    if not row or not (row['symbol'] or row['name']):
        try:
            rpc_url=os.environ.get('MARKET_RPC_HTTP_URL') or os.environ.get('RPC_HTTP_URL') or 'https://robinhood-rpc.publicnode.com'
            fresh=metadata(db,RPC(rpc_url,attempts=1),alert['asset'])
            row={'symbol':fresh.get('symbol'),'name':fresh.get('name')} if fresh else None
        except (OSError,ValueError,sqlite3.Error):
            row=None
    if row and (row['symbol'] or row['name']):
        e['symbol']=e.get('symbol') or row['symbol'];e['name']=e.get('name') or row['name']
    try:
        market=db.execute('SELECT * FROM market_snapshots WHERE asset=?',(alert['asset'],)).fetchone()
    except sqlite3.OperationalError:
        market=None
    if market:
        for key in ('symbol','name','quote_symbol','quote_decimals','price_quote','price_usd','market_cap_quote','market_cap_usd','liquidity_quote','liquidity_usd','volume_5m_quote','volume_1h_quote','volume_24h_quote','change_5m','change_1h','change_6h','change_24h'):
            if market[key] is not None:e[key]=market[key]
        e['market_source']=market['source'];e['market_status']=market['status']
    alert['evidence']=json.dumps(e,separators=(',',':'))
    try:
        with db:db.execute('UPDATE alerts SET evidence=? WHERE id=?',(alert['evidence'],alert['id']))
    except sqlite3.OperationalError:
        pass
    return alert,bool(e.get('symbol') or e.get('name'))


def deliver(db, token=None, chat_id=None, limit=10):
    if not token or not chat_id:return 0
    rows=db.execute("SELECT a.* FROM alert_deliveries d JOIN alerts a ON a.id=d.alert_id WHERE d.status IN ('pending','retry') AND d.attempts<5 ORDER BY a.id LIMIT ?",(limit,)).fetchall();sent=0
    for raw_alert in rows:
        alert,identified=hydrate_metadata(db,raw_alert);e=json.loads(alert['evidence'])
        market_ready=bool(e.get('price_quote') and e.get('market_cap_quote') is not None and e.get('liquidity_quote') is not None and e.get('quote_decimals') is not None)
        ready=identified and market_ready
        created=dt.datetime.fromisoformat(alert['created_at'])
        age=(dt.datetime.now(dt.timezone.utc)-created).total_seconds()
        if not ready:
            if age<300:continue
            with db:db.execute("UPDATE alert_deliveries SET status='suppressed',error='identity or market snapshot unresolved after 5 minutes' WHERE alert_id=?",(alert['id'],))
            continue
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


def deliver_milestones(db, token=None, chat_id=None, limit=10):
    if not token or not chat_id:return 0
    fresh_cutoff=(dt.datetime.now(dt.timezone.utc)-dt.timedelta(minutes=10)).isoformat()
    with db:
        db.execute("UPDATE alert_milestones SET status='stale',error='milestone notification window expired' WHERE status IN ('pending','retry') AND reached_at<?",(fresh_cutoff,))
    rows=db.execute("SELECT m.*,a.evidence,l.entry_price_quote,l.ath_price_quote,t.total_supply,t.decimals,t.symbol AS metadata_symbol,t.name AS metadata_name,s.symbol AS snapshot_symbol,s.quote_symbol AS snapshot_quote_symbol FROM alert_milestones m JOIN alerts a ON a.id=m.alert_id LEFT JOIN alert_lifecycle l ON l.alert_id=m.alert_id LEFT JOIN token_metadata t ON t.address=m.asset LEFT JOIN market_snapshots s ON s.asset=m.asset WHERE m.status IN ('pending','retry') AND m.reached_at>=? ORDER BY m.id LIMIT ?",(fresh_cutoff,max(limit,100))).fetchall();sent=0
    for row in rows:
        evidence=json.loads(row['evidence']);raw_symbol=evidence.get('symbol') or row['metadata_symbol'] or row['snapshot_symbol'];symbol=html.escape(raw_symbol or '')
        asset=html.escape(row['asset']);gmgn=f"https://gmgn.ai/robinhood/token/{row['asset']}";quote=html.escape(evidence.get('quote_symbol') or row['snapshot_quote_symbol'] or 'quote')
        try:supply=int(row['total_supply'])/(10**int(row['decimals']));first_mc=row['entry_price_quote']*supply
        except (TypeError,ValueError):first_mc=None
        fx=native_usd() if quote.upper() in ('ETH','WETH') else None
        first_usd=evidence.get('market_cap_usd')
        if first_usd is None and evidence.get('market_cap_quote') is not None and fx:first_usd=float(evidence['market_cap_quote'])*fx
        if first_usd is None and first_mc is not None and fx:first_usd=first_mc*fx
        peak_usd=first_usd*(1+float(row['peak_return'])/100) if first_usd is not None else None
        # A milestone without identity and alert-time valuation is not
        # actionable. Keep it pending briefly so reconciliation can fill both.
        if not symbol or first_usd is None or first_usd<=0:
            continue
        compact=lambda v:'—' if v is None else f'${v/1_000_000:.2f}m' if v>=1_000_000 else f'${v/1_000:.1f}k' if v>=1_000 else f'${v:.2f}'
        message=(f"🏁 <b>${symbol} MILESTONE {row['multiple']}X</b>\n\n"
                 f"First alert MC: <b>{compact(first_usd)}</b>\n"
                 f"Peak MC: <b>{compact(peak_usd)}</b> · <b>+{row['peak_return']:.1f}%</b>\n"
                 f"CA\n<code>{asset}</code>\n\n<a href=\"{gmgn}\">📈 Open token di GMGN</a>")
        try:
            body=parse.urlencode({'chat_id':chat_id,'text':message,'parse_mode':'HTML','disable_web_page_preview':'true'}).encode()
            with request.urlopen(request.Request(f'https://api.telegram.org/bot{token}/sendMessage',data=body),timeout=12) as response:
                if response.status!=200:raise OSError(f'Telegram HTTP {response.status}')
            with db:db.execute("UPDATE alert_milestones SET status='sent',delivered_at=?,error=NULL WHERE id=?",(now(),row['id']))
            sent+=1
        except Exception as exc:
            with db:db.execute("UPDATE alert_milestones SET status='retry',error=? WHERE id=?",(str(exc)[:300],row['id']))
    return sent


def matches(c):
    out=[]
    identified=bool((c.get('symbol') or '').strip() or (c.get('name') or '').strip()) and c.get('market_status') not in (None,'unknown')
    market_survived=(c.get('market_observations',0)>=2 and c.get('observation_span_seconds',0)>=180
                     and (c.get('change_5m') is None or c['change_5m']>=-20)
                     and (c.get('drawdown_from_observed_high') is None or c['drawdown_from_observed_high']>=-35))
    live_flow=(c.get('buys_5m',0)>=3 and c.get('buys_5m',0)>=c.get('sells_5m',0)
               and c.get('buy_sell_ratio',0)>=1.25)
    market_cap=c.get('market_cap_usd') or c.get('gmgn_market_cap_usd') or 0
    liquidity=c.get('liquidity_usd') or c.get('gmgn_liquidity_usd') or 0
    proof=c.get('consensus_proof') or []
    established_market=(identified and c.get('market_status')=='indexed-market'
                        and 50_000<=market_cap<=2_000_000 and liquidity>=5_000
                        and c.get('market_observations',0)>=3 and c.get('observation_span_seconds',0)>=300
                        and (c.get('drawdown_from_observed_high') is None or c['drawdown_from_observed_high']>=-25)
                        and c.get('change_5m') is not None and c['change_5m']>=0
                        and c.get('buys_5m',0)>=5 and c.get('buys_5m',0)>=c.get('sells_5m',0)*1.3
                        and not c.get('dev_exit_detected') and not c.get('insider_exit_detected')
                        and c.get('distribution_classification') not in ('possible-bundled-launch','creator-clustered-supply')
                        and c.get('creator_classification')!='toxic-history' and c.get('safety_status')!='higher-risk')
    fresh_alpha=(identified and bool(c.get('deployer')) and c.get('age_blocks') is not None and c['age_blocks']<=15000
                 and market_survived and live_flow and (c.get('last_trade_age_seconds') is None or c['last_trade_age_seconds']<=180)
                 and not c.get('dev_exit_detected') and not c.get('insider_exit_detected') and c.get('distribution_classification') not in ('possible-bundled-launch','creator-clustered-supply') and c.get('creator_classification')!='toxic-history' and (c.get('deployer_launch_count',0)<3 or c.get('creator_classification') in ('proven-runner','promising-history')) and c['safety_status']!='higher-risk')
    # Launchpad flow is available before token metadata, external market snapshots,
    # creator attribution, and contract reads. Surface a strong sustained imbalance
    # immediately; enriched rules can confirm it later without blocking discovery.
    onchain_breakout=(c.get('protocol') in ('curve','pons_v2','long')
                      and c.get('age_blocks') is not None and 25<=c['age_blocks']<=3000
                      and c.get('buys_5m',0)>=8 and c.get('buys_5m',0)-c.get('sells_5m',0)>=5
                      and c.get('buy_sell_ratio',0)>=1.3 and c.get('unique_buyers',0)>=5
                      and (c.get('last_trade_age_seconds') is None or c['last_trade_age_seconds']<=30)
                      and c.get('activity_score',0)>=40
                      and not c.get('dev_exit_detected') and not c.get('insider_exit_detected')
                      and c.get('distribution_classification') not in ('possible-bundled-launch','creator-clustered-supply')
                      and c.get('creator_classification')!='toxic-history' and c.get('safety_status')!='higher-risk')
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
    if onchain_breakout and not fresh_alpha:
        score=min(100,round(c.get('activity_score',0)+min(20,(c['buys_5m']-c['sells_5m'])*.5),1))
        out.append(('onchain-flow-breakout','high','Launchpad flow breakout terdeteksi',score))
    mature_consensus=(c.get('profitable_wallets_30m',0)>=2 and c.get('profitable_wallets_15m',0)>=1
                      and c.get('independent_profitable_wallets_30m',0)>=2)
    if established_market and mature_consensus:
        score=min(100,62+c.get('profitable_wallets_5m',0)*7+c.get('profitable_wallets_15m',0)*5+
                  c.get('independent_profitable_wallets_30m',0)*5)
        out.append(('established-smart-money','high','Smart money masuk ke market terkonfirmasi',score))
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
    if not eligible:
        eligible=list(latest.items())
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
    keys=('protocol','activity_score','conviction_score','safety_score','safety_status','buys','sells','buys_5m','sells_5m','last_trade_age_seconds','dev_buy_count','dev_sell_count','dev_exit_detected','deployer','creator_attribution','creator_confidence','insider_wallets','insider_sell_count','insider_exit_detected','creator_cluster_share','early_recipients','early_buyers','creator_linked_early_buyers','same_block_buyers','similar_size_buyers','shared_funding_clusters','bundle_score','distribution_classification','creator_launches','creator_indexed_assets','creator_survivors','creator_runners','creator_rugs','creator_runner_rate','creator_rug_rate','creator_median_peak_multiple','creator_reputation_score','creator_classification','creator_confidence','social_website','social_x_url','social_telegram_url','social_discord_url','social_source_count','social_cross_linked','social_score','social_confidence','social_status','deployer_launch_count','deployer_other_assets','unique_buyers','repeat_buyers','unique_senders','routed_share','smart_wallets','best_wallet_score','cluster_count','cluster_members','ordered_repeat_wallets','increasing_size_wallets','retained_wallets','provisional_funding_roots','shared_sender_wallets','shared_sender_clusters','migrating_wallets','migration_sources','fastest_migration_seconds','qualified_migrating_wallets_5m','activity_acceleration','age_blocks','buy_sell_ratio','market_observations','observation_span_seconds','drawdown_from_observed_high','safety_findings','symbol','name','quote_symbol','quote_decimals','price_quote','price_usd','market_cap_quote','market_cap_usd','liquidity_quote','liquidity_usd','volume_5m_quote','volume_1h_quote','volume_24h_quote','change_5m','change_1h','change_6h','change_24h','market_source','market_status','gmgn_first_seen_at','gmgn_last_seen_at','gmgn_price_usd','gmgn_market_cap_usd','gmgn_liquidity_usd','gmgn_holder_count','gmgn_security_status','gmgn_price_delta_pct','gmgn_market_cap_delta_pct','gmgn_liquidity_delta_pct','launch_stage','launch_created_at','launch_creator','launch_first_buy_at','launch_seconds_to_first_buy','launch_buys','launch_sells','launch_unique_buyers','launch_curve_progress_pct','launch_migrated_at','launch_seconds_to_migration','launch_metadata_seen_at','launch_gmgn_seen_at','profitable_wallets_5m','profitable_wallets_15m','profitable_wallets_30m','independent_profitable_wallets_30m','unattributed_profitable_wallets_30m','consensus_proof')
    keys=keys+('launch_protocol',)
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
    return alert['rule'] in TELEGRAM_ALPHA_RULES


def suppress_untracked_risk_deliveries(db):
    rows=db.execute("SELECT d.alert_id FROM alert_deliveries d JOIN alerts a ON a.id=d.alert_id WHERE d.status IN ('pending','retry')").fetchall()
    suppressed=0
    with db:
        for row in rows:
            if not telegram_worthy(db,row['alert_id']):
                db.execute("UPDATE alert_deliveries SET status='suppressed',error='dashboard-only rule; Telegram reserved for confirmed alpha' WHERE alert_id=?",(row['alert_id'],))
                suppressed+=1
    return suppressed


def cycle(path, cooldown=1800, improvement=8):
    db=database(path);schema(db)
    try:
        snapshot=read(path)
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
        milestone_delivered=deliver_milestones(db,os.environ.get('TELEGRAM_BOT_TOKEN'),os.environ.get('TELEGRAM_CHAT_ID'))
        if emitted:tracked=track_lifecycle(db)
        with db:
            set_meta(db,'alert_heartbeat',now())
            set_meta(db,'alert_active_rules',db.execute('SELECT COUNT(*) FROM alert_states WHERE active=1').fetchone()[0])
            set_meta(db,'alert_last_emitted',len(emitted))
            set_meta(db,'alert_lifecycles',tracked)
            set_meta(db,'telegram_configured','1' if os.environ.get('TELEGRAM_BOT_TOKEN') and os.environ.get('TELEGRAM_CHAT_ID') else '0')
            set_meta(db,'telegram_last_delivered',delivered)
            set_meta(db,'telegram_milestones_delivered',milestone_delivered)
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
