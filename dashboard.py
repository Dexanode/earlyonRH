"""Private read-only dashboard. Bind to loopback or use the Compose SSH tunnel."""
import argparse
import datetime as dt
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import sqlite3
import statistics
from urllib.parse import urlparse, parse_qs

STATIC = Path(__file__).parent / 'web'


def score_candidate(c, head):
    """Transparent 0-100 discovery score; activity evidence, not a buy signal."""
    trades = c['buys'] + c['sells']
    sample = min(1, trades / 10)
    diversity = min(25, c['unique_buyers'] * 2.5)
    repeats = min(15, c['repeat_buyers'] * 3)
    pressure = (20 * c['buys'] / trades * sample) if trades else 0
    baseline = c['events_previous_400'] / 4
    acceleration = c['events_last_100'] / max(1, baseline)
    activity = min(20, 8 * acceleration) * min(1, c['events_last_100'] / 5)
    age_blocks = max(0, head - c['launch_block']) if head is not None and c.get('launch_block') is not None else None
    early = 10 if age_blocks is not None and age_blocks <= 3000 else 5 if age_blocks is not None and age_blocks <= 10000 else 0
    sell_penalty = min(15, 15 * c['sells'] / max(1, c['buys']))
    c.update(score=round(max(0, min(100, diversity + repeats + pressure + activity + early - sell_penalty)), 1),
             age_blocks=age_blocks, buy_sell_ratio=round(c['buys'] / max(1, c['sells']), 2),
             activity_acceleration=round(acceleration, 2))
    return c


def age(value):
    if not value: return None
    try: return max(0, (dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(value)).total_seconds())
    except (ValueError, TypeError): return None


def calibration(db, tables):
    if not {'alerts','alert_lifecycle'}.issubset(tables):return {'rules':[],'totals':{'tracked':0,'mature_1h':0,'rules':0}}
    all_rows=db.execute('SELECT a.rule,a.severity,a.created_at,l.* FROM alerts a JOIN alert_lifecycle l ON l.alert_id=a.id').fetchall()
    rows=[r for r in all_rows if abs((dt.datetime.fromisoformat(r['tracking_started_at'])-dt.datetime.fromisoformat(r['created_at'])).total_seconds())<=60];groups={}
    for row in rows:groups.setdefault(row['rule'],[]).append(row)
    result=[]
    for rule,items in groups.items():
        metric=lambda key:[float(r[key]) for r in items if r[key] is not None]
        one=metric('return_1h');maxes=metric('max_return');drawdowns=metric('drawdown_from_ath')
        hit=lambda target:round(100*sum(v>=target for v in maxes)/len(maxes),1) if maxes else None
        win=round(100*sum(v>0 for v in one)/len(one),1) if one else None
        recommendation='collecting-data' if len(one)<20 else 'consider-tightening' if hit(25)>=35 and win>=55 else 'raise-threshold' if hit(25)<15 or win<40 else 'keep-threshold'
        result.append({'rule':rule,'severity':items[0]['severity'],'tracked':len(items),'mature_1h':len(one),'win_rate_1h':win,
          'median_5m':round(statistics.median(metric('return_5m')),2) if metric('return_5m') else None,'median_15m':round(statistics.median(metric('return_15m')),2) if metric('return_15m') else None,
          'median_1h':round(statistics.median(one),2) if one else None,'median_6h':round(statistics.median(metric('return_6h')),2) if metric('return_6h') else None,
          'hit_25':hit(25),'hit_50':hit(50),'hit_100':hit(100),'median_drawdown':round(statistics.median(drawdowns),2) if drawdowns else None,'recommendation':recommendation})
    result.sort(key=lambda x:(x['mature_1h'],x['tracked']),reverse=True)
    return {'rules':result,'totals':{'tracked':len(all_rows),'eligible':len(rows),'legacy':len(all_rows)-len(rows),'mature_1h':sum(x['mature_1h'] for x in result),'rules':len(result)},
            'method':{'win':'1h return > 0%','hits':'Maximum observed return since tracking began','minimum_sample':20,'price_unit':'Quote-token price; never mixed across assets'}}


def read(dbpath, asset=None, offset=0):
    empty = {'health': {'state': 'waiting', 'reason': 'Database listener belum tersedia.'}, 'candidates': [], 'events': [], 'births': [], 'protocols': [], 'sample_limit': 5000}
    if not Path(dbpath).exists(): return empty
    db = sqlite3.connect(Path(dbpath).resolve().as_uri()+'?mode=ro', uri=True, timeout=3)
    db.row_factory = sqlite3.Row
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {'events','meta'}.issubset(tables): return empty
        meta = dict(db.execute('SELECT key,value FROM meta'))
        freshness = age(meta.get('heartbeat') or meta.get('last_success'))
        state = 'waiting' if freshness is None else 'stale' if freshness > 60 else 'degraded' if meta.get('status') == 'degraded' else 'healthy'
        head = int(meta['head']) if meta.get('head') else None
        cursor = int(meta['cursor']) if meta.get('cursor') else None
        if state == 'healthy' and head is not None and cursor is not None and head-cursor > 100: state = 'catching-up'
        safe = {k:meta.get(k) for k in ('status','cursor','head','start','last_success','heartbeat','last_reorg','last_error')}
        health = dict(safe, state=state, age_seconds=freshness, lag_blocks=max(0,head-cursor) if head is not None and cursor is not None else None,
                      reason='Menunggu checkpoint pertama.' if freshness is None else '', chain_id=4663)
        health.update(alert_heartbeat=meta.get('alert_heartbeat'),alert_age_seconds=age(meta.get('alert_heartbeat')),
                      alert_active_rules=int(meta.get('alert_active_rules','0')),alert_last_emitted=int(meta.get('alert_last_emitted','0')),alert_lifecycles=int(meta.get('alert_lifecycles','0')))
        health.update(telegram_configured=meta.get('telegram_configured')=='1',telegram_last_delivered=int(meta.get('telegram_last_delivered','0')))
        if meta.get('transport') == 'websocket-logs':
            gap = max(0, int(meta.get('recovery_target', '0')) - int(meta.get('recovery_next', '1')) + 1)
            health.update(transport='websocket-logs', recovery_blocks=gap, validation=meta.get('validation'))
            if state == 'healthy' and gap:
                health['state'] = 'recovering-history'
            health['uncovered_from'] = int(meta['uncovered_from']) if meta.get('uncovered_from') else None
            health['uncovered_to'] = int(meta['uncovered_to']) if meta.get('uncovered_to') else None
            health['reason'] = f'Subscription event langsung; {gap:,} blok celah reconnect belum dipulihkan. Event ditahan tiga head, tanpa verifikasi header/receipt terpisah.'
            if health['uncovered_from']:
                health['reason'] += f' Coverage gap lama: blok {health["uncovered_from"]:,}–{health["uncovered_to"]:,}.'
            if meta.get('recovery_error'):
                health['reason'] += ' Recovery: ' + meta['recovery_error']
        recent = db.execute('SELECT tx_hash,asset,kind,name,decoded,block_number,observed_at,event_timestamp FROM events ORDER BY block_number DESC,log_index DESC LIMIT 5000').fetchall()
        profitable={r['wallet']:dict(r) for r in db.execute('SELECT wallet,win_rate,realized_assets,realized_by_quote FROM wallet_performance WHERE win_rate>=55 AND realized_assets>=3')} if 'wallet_performance' in tables else {}
        tx_rel={r['tx_hash']:r['relation'] for r in db.execute('SELECT tx_hash,relation FROM tx_attributions WHERE error IS NULL')} if 'tx_attributions' in tables else {}
        reference_ts=max((r['event_timestamp'] or 0 for r in recent),default=0)
        consensus={}
        for row in recent:
            if row['name']!='CurveBuy' or not row['asset']:continue
            wallet=(json.loads(row['decoded']).get('buyer') or '').lower()
            if wallet not in profitable:continue
            item=consensus.setdefault(row['asset'],{'w5':set(),'w15':set(),'w30':set(),'direct30':set(),'unknown30':set(),'proof':[]})
            seconds=max(0,reference_ts-(row['event_timestamp'] or 0))
            if seconds<=300:item['w5'].add(wallet)
            if seconds<=900:item['w15'].add(wallet)
            if seconds<=1800:
                item['w30'].add(wallet)
                relation=tx_rel.get(row['tx_hash'])
                if relation=='direct':item['direct30'].add(wallet)
                elif relation not in ('direct','routed'):item['unknown30'].add(wallet)
                if len(item['proof'])<10:item['proof'].append({'wallet':wallet,'tx_hash':row['tx_hash'],'block':row['block_number'],'timestamp':row['event_timestamp'],'relation':tx_rel.get(row['tx_hash']) or 'unknown','win_rate':profitable[wallet]['win_rate'],'realized_assets':profitable[wallet]['realized_assets']})
        launches = {r['asset']: r['created_block'] for r in db.execute('SELECT asset,MIN(created_block) created_block FROM watches WHERE asset IS NOT NULL GROUP BY asset')}
        launch_deployers={r['target']:r['source'].lower() for r in db.execute("SELECT source,target FROM topology_edges WHERE relation='deployed'")} if 'topology_edges' in tables else {}
        candidates = {}
        for row in recent:
            key = row['asset']
            if not key: continue
            c = candidates.setdefault(key, {'id':key,'kind':'pool' if row['kind']=='v4' else 'token','protocol':row['kind'],'events':0,'buys':0,'sells':0,'buys_5m':0,'sells_5m':0,'liquidity_changes':0,'last_block':row['block_number'],'last_trade_timestamp':None,'last_seen':row['observed_at'],'first_seen_in_sample':row['observed_at'],'latest_event':row['name'],'currencies':[],'buyers':{},'trade_wallets':[],'events_last_100':0,'events_previous_400':0,'launch_block':launches.get(key)})
            c['events'] += 1
            c['first_seen_in_sample'] = min(c['first_seen_in_sample'],row['observed_at'])
            c['buys'] += row['name']=='CurveBuy'
            c['sells'] += row['name']=='CurveSell'
            event_age=max(0,reference_ts-(row['event_timestamp'] or 0)) if reference_ts else None
            if event_age is not None and event_age<=300:
                c['buys_5m'] += row['name']=='CurveBuy';c['sells_5m'] += row['name']=='CurveSell'
            c['liquidity_changes'] += row['name']=='ModifyLiquidity'
            if head is not None:
                c['events_last_100'] += row['block_number'] > head-100
                c['events_previous_400'] += head-500 < row['block_number'] <= head-100
            values=json.loads(row['decoded'])
            buyer=values.get('buyer') if row['name']=='CurveBuy' else None
            if buyer: c['buyers'][buyer]=c['buyers'].get(buyer,0)+1
            if row['name'] in ('CurveBuy','CurveSell'):
                wallet=values.get('buyer') if row['name']=='CurveBuy' else values.get('seller')
                c['last_trade_timestamp']=max(c['last_trade_timestamp'] or 0,row['event_timestamp'] or 0)
                if wallet:c['trade_wallets'].append((row['name'],wallet.lower()))
            if row['name']=='Initialize':
                c['currencies']=[values.get('currency0'),values.get('currency1')]
        ranked=[]
        analyses = {r['asset']: dict(r) for r in db.execute('SELECT * FROM asset_analysis')} if 'asset_analysis' in tables else {}
        attribution = {}
        if 'tx_attributions' in tables:
            for row in db.execute("SELECT asset,sender,relation FROM tx_attributions WHERE error IS NULL"):
                a=attribution.setdefault(row['asset'],{'senders':set(),'direct':0,'routed':0})
                if row['sender']: a['senders'].add(row['sender'])
                a[row['relation']] = a.get(row['relation'],0)+1
        wallet_signal={}
        if 'wallet_asset_stats' in tables and 'wallet_profiles' in tables:
            for row in db.execute('SELECT s.asset,SUM(p.smart_score>=55) smart_wallets,MAX(p.smart_score) best_wallet_score FROM wallet_asset_stats s JOIN wallet_profiles p ON p.wallet=s.wallet GROUP BY s.asset'):
                wallet_signal[row['asset']]=dict(row)
        cluster_signal={}
        if 'wallet_clusters' in tables:
            cluster_signal={r['asset']:dict(r) for r in db.execute('SELECT asset,COUNT(*) cluster_count,MAX(members) cluster_members FROM wallet_clusters GROUP BY asset')}
        capital_signal={}
        if 'capital_wallet_asset' in tables:
            for row in db.execute("SELECT asset,COUNT(*) wallets,SUM(buy_count>=2) ordered_repeat_wallets,SUM(COALESCE(size_trend,0)>1) increasing_size_wallets,SUM(retained_raw!='0') retained_wallets,COUNT(DISTINCT funding_root) provisional_funding_roots,SUM(funding_confidence='observed-shared-sender') shared_sender_wallets FROM capital_wallet_asset GROUP BY asset"):
                capital_signal[row['asset']]=dict(row)
        shared_clusters={}
        if 'capital_clusters' in tables:
            shared_clusters={r['asset']:r['n'] for r in db.execute("SELECT asset,COUNT(*) n FROM capital_clusters WHERE confidence='observed-shared-sender' AND members>=2 GROUP BY asset")}
        migration_signal={}
        if 'capital_migrations' in tables:
            join='LEFT JOIN wallet_performance p ON p.wallet=m.wallet' if 'wallet_performance' in tables else ''
            qualified="COUNT(DISTINCT CASE WHEN m.target_buy_time>=strftime('%s','now')-300 AND p.win_rate>=55 AND p.realized_assets>=3 THEN m.wallet END)" if join else '0'
            sql=f"SELECT m.target_asset,COUNT(DISTINCT m.wallet) migrating_wallets,COUNT(DISTINCT m.source_asset) migration_sources,MIN(m.latency_seconds) fastest_migration_seconds,{qualified} qualified_migrating_wallets_5m FROM capital_migrations m {join} GROUP BY m.target_asset"
            migration_signal={r['target_asset']:dict(r) for r in db.execute(sql)}
        markets={r['asset']:dict(r) for r in db.execute('SELECT * FROM market_snapshots')} if 'market_snapshots' in tables else {}
        for c in candidates.values():
            c['unique_buyers']=len(c['buyers'])
            c['repeat_buyers']=sum(v>1 for v in c['buyers'].values())
            del c['buyers']
            score_candidate(c, head)
            c['activity_score']=c.pop('score')
            analysis=analyses.get(c['id'])
            a=attribution.get(c['id'],{'senders':set(),'direct':0,'routed':0})
            c['unique_senders']=len(a['senders'])
            c['routed_share']=round(a['routed']/max(1,a['direct']+a['routed']),2)
            ws=wallet_signal.get(c['id'],{});cl=cluster_signal.get(c['id'],{})
            c['smart_wallets']=ws.get('smart_wallets',0);c['best_wallet_score']=ws.get('best_wallet_score')
            c['cluster_count']=cl.get('cluster_count',0);c['cluster_members']=cl.get('cluster_members',0)
            cap=capital_signal.get(c['id'],{})
            for key in ('ordered_repeat_wallets','increasing_size_wallets','retained_wallets','provisional_funding_roots','shared_sender_wallets'):
                c[key]=cap.get(key,0) or 0
            c['shared_sender_clusters']=shared_clusters.get(c['id'],0)
            mig=migration_signal.get(c['id'],{})
            c['migrating_wallets']=mig.get('migrating_wallets',0);c['migration_sources']=mig.get('migration_sources',0);c['fastest_migration_seconds']=mig.get('fastest_migration_seconds');c['qualified_migrating_wallets_5m']=mig.get('qualified_migrating_wallets_5m',0)
            flow=consensus.get(c['id'],{});c['profitable_wallets_5m']=len(flow.get('w5',()))
            c['profitable_wallets_15m']=len(flow.get('w15',()));c['profitable_wallets_30m']=len(flow.get('w30',()))
            c['independent_profitable_wallets_30m']=len(flow.get('direct30',()));c['unattributed_profitable_wallets_30m']=len(flow.get('unknown30',()));c['consensus_proof']=flow.get('proof',[])
            market=markets.get(c['id'],{})
            for key in ('symbol','name','quote_symbol','quote_decimals','price_quote','price_usd','market_cap_quote','market_cap_usd','liquidity_quote','liquidity_usd','volume_5m_quote','volume_1h_quote','volume_24h_quote','change_5m','change_1h','change_6h','change_24h','source','status'):
                c['market_'+key if key in ('source','status') else key]=market.get(key)
            c['safety_score']=analysis['safety_score'] if analysis else None
            c['safety_status']=analysis['safety_status'] if analysis else 'unknown'
            c['safety_findings']=json.loads(analysis['findings']) if analysis else []
            c['deployer']=(analysis['deployer'] if analysis and analysis['deployer'] else launch_deployers.get(c['id']))
            deployer=(c['deployer'] or '').lower()
            c['dev_buy_count']=sum(name=='CurveBuy' and wallet==deployer for name,wallet in c['trade_wallets']) if deployer else 0
            c['dev_sell_count']=sum(name=='CurveSell' and wallet==deployer for name,wallet in c['trade_wallets']) if deployer else 0
            c['dev_exit_detected']=c['dev_sell_count']>0
            c['last_trade_age_seconds']=max(0,reference_ts-c['last_trade_timestamp']) if reference_ts and c['last_trade_timestamp'] else None
            del c['trade_wallets']
            c['owner']=analysis['owner'] if analysis else None
            c['proxy']=bool(analysis and (analysis['implementation'] or analysis['proxy_admin']))
            c['contract_analyzed_at']=analysis['analyzed_at'] if analysis else None
            c['conviction_score']=round(.65*c['activity_score']+.35*c['safety_score']-10*c['routed_share'],1) if analysis else None
            c['conviction_score']=max(0,min(100,c['conviction_score'])) if c['conviction_score'] is not None else None
            c['verdict']='observe' if c['conviction_score'] is None else 'trench-candidate' if c['conviction_score']>=70 and c['safety_status']=='screened' else 'watch' if c['conviction_score']>=45 else 'avoid'
            ranked.append(c)
        ranked.sort(key=lambda c:(c['conviction_score'] is not None,c['conviction_score'] or c['activity_score'],c['last_block']),reverse=True)
        health['events_in_sample']=len(recent)
        health['candidates_in_sample']=len(candidates)
        health['decode_errors_in_sample']=sum(r['name']=='DecodeError' for r in recent)
        events=[]; wallets=[]; clusters=[]; capital_flows=[]; funding_clusters=[]; migrations=[]; has_more=False
        if asset:
            rows=db.execute('SELECT * FROM events WHERE asset=? ORDER BY block_number DESC,log_index DESC LIMIT 101 OFFSET ?', (asset,offset)).fetchall()
            has_more=len(rows)>100
            for row in rows[:100]:
                e=dict(row);e['decoded']=json.loads(e['decoded']);e.pop('raw')
                e['explorer_url']='https://robinhoodchain.blockscout.com/tx/'+e['tx_hash']
                events.append(e)
            if 'wallet_asset_stats' in tables and 'wallet_profiles' in tables:
                pnl_join='LEFT JOIN wallet_asset_pnl ap ON ap.asset=s.asset AND ap.wallet=s.wallet LEFT JOIN wallet_performance wp ON wp.wallet=s.wallet' if {'wallet_asset_pnl','wallet_performance'}.issubset(tables) else ''
                pnl_cols=',ap.realized_pnl_quote,ap.quote_symbol pnl_quote_symbol,ap.position_tokens,wp.win_rate,wp.realized_assets,wp.realized_by_quote,wp.coverage pnl_coverage' if pnl_join else ''
                wallets=[dict(r) for r in db.execute(f'SELECT s.*,p.smart_score,p.assets,p.early_assets,p.buys total_buys,p.sells total_sells{pnl_cols} FROM wallet_asset_stats s JOIN wallet_profiles p ON p.wallet=s.wallet {pnl_join} WHERE s.asset=? ORDER BY p.smart_score DESC,s.buys+s.sells DESC LIMIT 50',(asset,))]
            if 'wallet_clusters' in tables:clusters=[dict(r) for r in db.execute('SELECT * FROM wallet_clusters WHERE asset=? ORDER BY members DESC,transactions DESC',(asset,))]
            if 'capital_wallet_asset' in tables:capital_flows=[dict(r) for r in db.execute('SELECT * FROM capital_wallet_asset WHERE asset=? ORDER BY buy_count DESC,size_trend DESC LIMIT 100',(asset,))]
            if 'capital_clusters' in tables:funding_clusters=[dict(r) for r in db.execute('SELECT * FROM capital_clusters WHERE asset=? ORDER BY members DESC,buys DESC LIMIT 50',(asset,))]
            if 'capital_migrations' in tables:migrations=[dict(r) for r in db.execute('SELECT * FROM capital_migrations WHERE target_asset=? ORDER BY target_buy_time DESC LIMIT 100',(asset,))]
        alerts=[]
        if 'alerts' in tables:
            for row in db.execute('SELECT * FROM alerts ORDER BY id DESC LIMIT 100'):
                item=dict(row);item['evidence']=json.loads(item['evidence'])
                life=db.execute('SELECT * FROM alert_lifecycle WHERE alert_id=?',(item['id'],)).fetchone() if 'alert_lifecycle' in tables else None
                item['lifecycle']=dict(life) if life else None;alerts.append(item)
        health.update(wallet_profiler_heartbeat=meta.get('wallet_profiler_heartbeat'),wallet_profiler_age_seconds=age(meta.get('wallet_profiler_heartbeat')),wallet_profiles=int(meta.get('wallet_profiles','0')),wallet_clusters=int(meta.get('wallet_clusters','0')))
        health.update(wallet_pnl_heartbeat=meta.get('wallet_pnl_heartbeat'),wallet_pnl_age_seconds=age(meta.get('wallet_pnl_heartbeat')),wallet_pnl_wallets=int(meta.get('wallet_pnl_wallets','0')))
        health.update(market_heartbeat=meta.get('market_heartbeat'),market_age_seconds=age(meta.get('market_heartbeat')),market_assets=int(meta.get('market_assets','0')))
        health.update(capital_flow_heartbeat=meta.get('capital_flow_heartbeat'),capital_flow_age_seconds=age(meta.get('capital_flow_heartbeat')),capital_flow_wallet_assets=int(meta.get('capital_flow_wallet_assets','0')),capital_flow_clusters=int(meta.get('capital_flow_clusters','0')),capital_migrations=int(meta.get('capital_migrations','0')))
        protocols=[dict(r) for r in db.execute('SELECT * FROM protocol_sources ORDER BY status,name')] if 'protocol_sources' in tables else []
        births=[]
        if 'topology_observations' in tables:
            for row in db.execute("SELECT * FROM topology_observations WHERE observation_type IN ('NEW_MARKET_BIRTH','NEW_POOL_BIRTH','MARKET_GRADUATED') ORDER BY event_timestamp DESC,id DESC LIMIT 100"):
                item=dict(row);item['payload']=json.loads(item['payload']);item['explorer_url']='https://robinhoodchain.blockscout.com/tx/'+item['tx_hash'];births.append(item)
        health.update(topology_heartbeat=meta.get('topology_heartbeat'),topology_age_seconds=age(meta.get('topology_heartbeat')),
                      topology_entities=int(meta.get('topology_entities','0')),topology_edges=int(meta.get('topology_edges','0')),
                      topology_births=int(meta.get('topology_births','0')))
        return {'health':health,'candidates':ranked,'events':events,'wallets':wallets,'clusters':clusters,'capital_flows':capital_flows,'funding_clusters':funding_clusters,'migrations':migrations,'alerts':alerts,'births':births,'protocols':protocols,'calibration':calibration(db,tables),'has_more':has_more,'offset':offset,'sample_limit':5000,
                'score_model': {'version':2,'meaning':'Screening evidence only; not a return prediction or buy recommendation.','sample':'Latest 5,000 stored events.','components':['activity score','budgeted sender attribution','contract screening'],'limitations':['Safety screening is not a source-code audit.','Unknown capabilities receive no safety points.','No USD liquidity, holder history, social, or profitable-wallet history.']}}
    finally: db.close()


def handler(dbpath):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed=urlparse(self.path)
            try:
                if parsed.path=='/api/radar':
                    q=parse_qs(parsed.query); asset=q.get('asset',[None])[0]
                    if asset and not re.fullmatch(r'0x(?:[0-9a-f]{40}|[0-9a-f]{64})',asset):
                        self.reply(400,b'{"error":"Invalid asset"}','application/json');return
                    offset=int(q.get('offset',['0'])[0])
                    if not 0<=offset<=1000000: raise ValueError()
                    payload=json.dumps(read(dbpath,asset,offset)).encode()
                    self.reply(200,payload,'application/json');return
                files={'/':('index.html','text/html'),'/app.js':('app.js','text/javascript'),'/style.css':('style.css','text/css')}
                if parsed.path not in files: self.reply(404,b'Not found','text/plain');return
                name,mime=files[parsed.path];self.reply(200,(STATIC/name).read_bytes(),mime)
            except ValueError: self.reply(400,b'{"error":"Invalid query"}','application/json')
            except sqlite3.Error: self.reply(503,b'{"error":"Database busy or unavailable; retry shortly"}','application/json')

        def reply(self,status,body,mime):
            self.send_response(status)
            self.send_header('Content-Type',mime+'; charset=utf-8')
            self.send_header('Content-Length',str(len(body)))
            self.send_header('Cache-Control','no-store')
            self.send_header('X-Content-Type-Options','nosniff')
            self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
            self.end_headers();self.wfile.write(body)
        def log_message(self,*args): pass
    return Handler


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--db',default='data/listener.sqlite');p.add_argument('--host',default='127.0.0.1');p.add_argument('--port',type=int,default=8080);a=p.parse_args()
    print(f'Dashboard: http://{a.host}:{a.port}',flush=True)
    ThreadingHTTPServer((a.host,a.port),handler(a.db)).serve_forever()
