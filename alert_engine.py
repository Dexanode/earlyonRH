"""Persistent, deduplicated alerts derived from the live radar evidence."""
import argparse
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
    ''')


def matches(c):
    out=[]
    if c['safety_status']=='higher-risk':
        out.append(('contract-risk','critical','Contract risk terdeteksi',c.get('safety_score') or 0))
    if (c.get('conviction_score') or 0)>=70 and c['safety_status']=='screened' and c['unique_senders']>=3 and c['buys']>=5 and c['buy_sell_ratio']>=1.5 and c['activity_acceleration']>=1.2 and c['routed_share']<=.75:
        out.append(('trench-candidate','high','Kandidat trench terkonfirmasi',c['conviction_score']))
    elif (c.get('conviction_score') or 0)>=58 and c['safety_status']=='screened' and c['unique_senders']>=2 and c['buys']>=5 and c['activity_acceleration']>=1.5:
        out.append(('momentum-watch','medium','Momentum awal mulai terbentuk',c['conviction_score']))
    elif c['activity_score']>=55 and c['safety_status']=='screened' and c['unique_buyers']>=3 and c['buys']>=5 and (c.get('age_blocks') is None or c['age_blocks']<=3000):
        out.append(('early-watch','low','Aktivitas awal layak dipantau',c['activity_score']))
    return out


def evidence(c):
    keys=('protocol','activity_score','conviction_score','safety_score','safety_status','buys','sells','unique_buyers','repeat_buyers','unique_senders','routed_share','activity_acceleration','age_blocks','buy_sell_ratio','safety_findings')
    return {k:c.get(k) for k in keys}


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
                    payload=evidence(c)
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
    if snapshot['health'].get('state')!='healthy': return []
    db=database(path);schema(db)
    try:
        emitted=evaluate(db,snapshot['candidates'],cooldown,improvement)
        with db:
            set_meta(db,'alert_heartbeat',now())
            set_meta(db,'alert_active_rules',db.execute('SELECT COUNT(*) FROM alert_states WHERE active=1').fetchone()[0])
            set_meta(db,'alert_last_emitted',len(emitted))
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
