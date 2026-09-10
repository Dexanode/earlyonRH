"""Private read-only dashboard. Bind to loopback or use the Compose SSH tunnel."""
import argparse
import datetime as dt
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import sqlite3
from urllib.parse import urlparse, parse_qs

STATIC = Path(__file__).parent / 'web'


def age(value):
    if not value: return None
    try: return max(0, (dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(value)).total_seconds())
    except (ValueError, TypeError): return None


def read(dbpath, asset=None, offset=0):
    empty = {'health': {'state': 'waiting', 'reason': 'Database listener belum tersedia.'}, 'candidates': [], 'events': [], 'sample_limit': 5000}
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
        recent = db.execute('SELECT asset,kind,name,decoded,block_number,observed_at,event_timestamp FROM events ORDER BY block_number DESC,log_index DESC LIMIT 5000').fetchall()
        candidates = {}
        for row in recent:
            key = row['asset']
            if not key: continue
            c = candidates.setdefault(key, {'id':key,'kind':'pool' if row['kind']=='v4' else 'token','protocol':row['kind'],'events':0,'buys':0,'sells':0,'liquidity_changes':0,'last_block':row['block_number'],'last_seen':row['observed_at'],'first_seen_in_sample':row['observed_at'],'latest_event':row['name'],'currencies':[]})
            c['events'] += 1
            c['first_seen_in_sample'] = min(c['first_seen_in_sample'],row['observed_at'])
            c['buys'] += row['name']=='CurveBuy'
            c['sells'] += row['name']=='CurveSell'
            c['liquidity_changes'] += row['name']=='ModifyLiquidity'
            if row['name']=='Initialize':
                v=json.loads(row['decoded']); c['currencies']=[v.get('currency0'),v.get('currency1')]
        health['events_in_sample']=len(recent)
        health['candidates_in_sample']=len(candidates)
        health['decode_errors_in_sample']=sum(r['name']=='DecodeError' for r in recent)
        events=[]; has_more=False
        if asset:
            rows=db.execute('SELECT * FROM events WHERE asset=? ORDER BY block_number DESC,log_index DESC LIMIT 101 OFFSET ?', (asset,offset)).fetchall()
            has_more=len(rows)>100
            for row in rows[:100]:
                e=dict(row);e['decoded']=json.loads(e['decoded']);e.pop('raw')
                e['explorer_url']='https://robinhoodchain.blockscout.com/tx/'+e['tx_hash']
                events.append(e)
        return {'health':health,'candidates':list(candidates.values()),'events':events,'has_more':has_more,'offset':offset,'sample_limit':5000}
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
