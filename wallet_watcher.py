"""Efficient live tracked-wallet watcher: one full-block RPC covers every wallet."""
import json
import logging
import os
from pathlib import Path
import sqlite3
import time

from listener import RPC, RpcError, RateLimited, database, get_meta, now, set_meta

LOG=logging.getLogger('wallet-watcher')


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS tracked_wallets(
        wallet TEXT PRIMARY KEY,name TEXT NOT NULL,emoji TEXT,source TEXT NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1,imported_at TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS tracked_wallet_activity(
        tx_hash TEXT PRIMARY KEY,wallet TEXT NOT NULL,wallet_name TEXT,
        block_number INTEGER NOT NULL,tx_to TEXT,value_raw TEXT,input_selector TEXT,
        observed_at TEXT NOT NULL,asset TEXT,side TEXT);
      CREATE INDEX IF NOT EXISTS tracked_activity_block ON tracked_wallet_activity(block_number DESC);
      CREATE TABLE IF NOT EXISTS tx_attributions(
        tx_hash TEXT PRIMARY KEY,asset TEXT NOT NULL,sender TEXT,event_actor TEXT,
        relation TEXT,checked_at TEXT NOT NULL,error TEXT);
    ''')


def load_registry(db,path='tracked_wallets.json'):
    rows=[]
    for item in json.loads(Path(path).read_text()):
        wallet=str(item.get('address','')).lower()
        if len(wallet)==42:rows.append((wallet,item.get('name') or wallet[:10],item.get('emoji'),'user-watchlist',now()))
    with db:db.executemany('INSERT INTO tracked_wallets(wallet,name,emoji,source,imported_at) VALUES(?,?,?,?,?) ON CONFLICT(wallet) DO UPDATE SET name=excluded.name,emoji=excluded.emoji,enabled=1',rows)
    return {r['wallet']:dict(r) for r in db.execute('SELECT wallet,name,emoji FROM tracked_wallets WHERE enabled=1')}


def ingest_block(db,rpc,number,tracked):
    block=rpc.call('eth_getBlockByNumber',[hex(number),True])
    if not block:return 0
    txs={tx['hash'].lower():tx for tx in block.get('transactions',[]) if isinstance(tx,dict) and tx.get('hash')}
    event_rows=db.execute('SELECT DISTINCT tx_hash,asset,name,decoded FROM events WHERE block_number=?',(number,)).fetchall()
    event_by_tx={r['tx_hash'].lower():r for r in event_rows}
    stamp=now();hits=0
    with db:
        for tx_hash,row in event_by_tx.items():
            tx=txs.get(tx_hash);sender=(tx.get('from') or '').lower() if tx else None
            if not sender:continue
            values=json.loads(row['decoded']);actor=values.get('buyer') or values.get('seller')
            relation='direct' if actor and sender==actor.lower() else 'routed'
            db.execute('INSERT OR REPLACE INTO tx_attributions VALUES(?,?,?,?,?,?,NULL)',(tx_hash,row['asset'],sender,actor,relation,stamp))
        for tx_hash,tx in txs.items():
            sender=(tx.get('from') or '').lower()
            if sender not in tracked:continue
            event=event_by_tx.get(tx_hash);side=None
            if event:side='buy' if event['name'] in ('CurveBuy','DexBuy') else 'sell' if event['name'] in ('CurveSell','DexSell') else None
            data=tx.get('input') or '0x'
            db.execute('INSERT OR REPLACE INTO tracked_wallet_activity VALUES(?,?,?,?,?,?,?,?,?)',(tx_hash,sender,tracked[sender]['name'],number,(tx.get('to') or '').lower() or None,str(int(tx.get('value','0x0'),16)),data[:10],stamp,event['asset'] if event else None,side))
            hits+=1
        set_meta(db,'wallet_watcher_cursor',number);set_meta(db,'wallet_watcher_heartbeat',stamp);set_meta(db,'wallet_watcher_last_hits',hits)
    return hits


def main():
    url=os.environ.get('RPC_HTTP_URL');db=database('data/live.sqlite');schema(db);tracked=load_registry(db)
    rpc=RPC(url,attempts=1,spacing=.5)
    while True:
        try:
            head=int(get_meta(db,'head') or 0);cursor=int(get_meta(db,'wallet_watcher_cursor') or max(0,head-200))
            if cursor<head:
                target=min(head,cursor+20);hits=0
                for number in range(cursor+1,target+1):hits+=ingest_block(db,rpc,number,tracked)
                LOG.info('scanned blocks %s-%s tracked_hits=%s',cursor+1,target,hits)
            else:time.sleep(2)
        except (RpcError,RateLimited,sqlite3.Error,ValueError) as exc:
            LOG.warning('watch delayed: %s',exc);time.sleep(3)


if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s');main()
