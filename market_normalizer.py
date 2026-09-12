"""Normalize token/curve market data from stored trades and cheap onchain reads."""
import argparse
import datetime as dt
import json
import logging
import os
import sqlite3
import time
import uuid
from urllib import parse, request

from listener import RPC, RpcError, database, now, set_meta

LOG=logging.getLogger('market-normalizer')
DECIMALS='0x313ce567';SYMBOL='0x95d89b41';NAME='0x06fdde03';SUPPLY='0x18160ddd';BALANCE='0x70a08231'
ZERO='0x'+'0'*40
DEXSCREENER='https://api.dexscreener.com/latest/dex/tokens/'
GMGN_OPENAPI='https://openapi.gmgn.ai/v1/token/info'


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS market_snapshots(
        asset TEXT PRIMARY KEY, updated_at TEXT NOT NULL, symbol TEXT, name TEXT,
        decimals INTEGER, quote_token TEXT, quote_symbol TEXT, quote_decimals INTEGER,
        price_quote REAL, price_usd REAL, market_cap_quote REAL, market_cap_usd REAL,
        liquidity_quote REAL, liquidity_usd REAL, volume_5m_quote REAL,
        volume_1h_quote REAL, volume_24h_quote REAL, change_5m REAL,
        change_1h REAL, change_6h REAL, change_24h REAL, source TEXT NOT NULL,
        status TEXT NOT NULL, error TEXT);
      CREATE TABLE IF NOT EXISTS token_metadata(
        address TEXT PRIMARY KEY, checked_at TEXT NOT NULL, symbol TEXT, name TEXT,
        decimals INTEGER, total_supply TEXT, error TEXT);
      CREATE TABLE IF NOT EXISTS market_observations(
        asset TEXT NOT NULL, observed_at TEXT NOT NULL, price_quote REAL,
        liquidity_quote REAL, volume_5m_quote REAL, change_5m REAL,
        PRIMARY KEY(asset,observed_at));
      CREATE INDEX IF NOT EXISTS market_observations_asset_time
        ON market_observations(asset,observed_at DESC);
    ''')


def uint(value):
    try:return int(value,16)
    except (TypeError,ValueError):return None


def abi_text(value):
    try:
        raw=bytes.fromhex(value.removeprefix('0x'))
        if len(raw)==32:return raw.rstrip(b'\0').decode('utf-8') or None
        if len(raw)>=64:
            offset=int.from_bytes(raw[:32],'big');size=int.from_bytes(raw[offset:offset+32],'big')
            return raw[offset+32:offset+32+size].decode('utf-8') or None
    except (AttributeError,ValueError,UnicodeDecodeError):pass
    return None


def call(rpc,address,data):return rpc.call('eth_call',[{'to':address,'data':data},'latest'])


def gmgn_metadata(address,timeout=8):
    """Fetch GMGN's normalized token identity when an API key is configured."""
    key=os.environ.get('GMGN_API_KEY')
    if not key:return {}
    query=parse.urlencode({'chain':'robinhood','address':address,'timestamp':int(time.time()),'client_id':str(uuid.uuid4())})
    req=request.Request(GMGN_OPENAPI+'?'+query,headers={'Accept':'application/json','X-APIKEY':key,'User-Agent':'earlyonRH/1'})
    with request.urlopen(req,timeout=timeout) as response:payload=json.load(response)
    if str(payload.get('code')) not in ('0','0.0'):return {}
    data=payload.get('data') or {}
    candidates=[]
    def walk(value):
        if isinstance(value,dict):
            if value.get('symbol') or value.get('name'):candidates.append(value)
            for child in value.values():walk(child)
        elif isinstance(value,list):
            for child in value:walk(child)
    walk(data)
    exact=next((item for item in candidates if str(item.get('address') or item.get('token_address') or '').lower()==address.lower()),None)
    chosen=exact or (candidates[0] if candidates else {})
    return {'symbol':chosen.get('symbol'),'name':chosen.get('name')}


def indexed_metadata(address,timeout=8):
    pool=indexed_pool(address,timeout)
    if not pool:return {}
    for token in (pool.get('baseToken') or {},pool.get('quoteToken') or {}):
        if str(token.get('address') or '').lower()==address.lower():
            return {'symbol':token.get('symbol'),'name':token.get('name')}
    return {}


def metadata(db,rpc,address):
    cached=db.execute('SELECT * FROM token_metadata WHERE address=?',(address,)).fetchone()
    if cached and not cached['error'] and cached['symbol'] and cached['name']:return dict(cached)
    if cached and not cached['error']:
        decimals=cached['decimals'];supply=uint(cached['total_supply']);symbol=cached['symbol'];name=cached['name']
    else:
        try:
            decimals=uint(call(rpc,address,DECIMALS));supply=uint(call(rpc,address,SUPPLY))
            symbol=abi_text(call(rpc,address,SYMBOL));name=abi_text(call(rpc,address,NAME))
            if decimals is None or not 0<=decimals<=36:raise RpcError('invalid decimals')
        except RpcError as exc:
            row=(address,now(),None,None,None,None,str(exc))
            with db:db.execute('INSERT OR REPLACE INTO token_metadata VALUES(?,?,?,?,?,?,?)',row)
            return dict(db.execute('SELECT * FROM token_metadata WHERE address=?',(address,)).fetchone())
    if not symbol or not name:
        for source in (gmgn_metadata,indexed_metadata):
            try:external=source(address)
            except (OSError,ValueError,json.JSONDecodeError):continue
            symbol=symbol or external.get('symbol');name=name or external.get('name')
            if symbol and name:break
    row=(address,now(),symbol,name,decimals,str(supply) if supply is not None else None,None)
    with db:db.execute('INSERT OR REPLACE INTO token_metadata VALUES(?,?,?,?,?,?,?)',row)
    return dict(db.execute('SELECT * FROM token_metadata WHERE address=?',(address,)).fetchone())


def pct(current,old):return round((current/old-1)*100,2) if current and old else None


def calculate(rows,token_decimals,quote_decimals,stamp):
    trades=[]
    for r in rows:
        v=json.loads(r['decoded'])
        quote=int(v.get('quoteIn') or v.get('quoteOut') or 0)/(10**quote_decimals)
        tokens=int(v.get('tokensOut') or v.get('tokensIn') or 0)/(10**token_decimals)
        if quote>0 and tokens>0:trades.append((int(r['event_timestamp']),quote/tokens,quote))
    if not trades:return {}
    current=trades[-1][1]
    def at(seconds):
        cutoff=stamp-seconds;eligible=[p for ts,p,_ in trades if ts<=cutoff]
        return eligible[-1] if eligible else None
    def volume(seconds):return round(sum(q for ts,_,q in trades if ts>=stamp-seconds),8)
    return {'price_quote':current,'volume_5m_quote':volume(300),'volume_1h_quote':volume(3600),'volume_24h_quote':volume(86400),
            'change_5m':pct(current,at(300)),'change_1h':pct(current,at(3600)),'change_6h':pct(current,at(21600)),'change_24h':pct(current,at(86400))}


def indexed_pool(asset, timeout=8):
    """Select the deepest matching Robinhood pool from a free market index."""
    req=request.Request(DEXSCREENER+asset,headers={'Accept':'application/json','User-Agent':'earlyonRH/1'})
    with request.urlopen(req,timeout=timeout) as response:payload=json.load(response)
    pairs=[]
    for pair in payload.get('pairs') or []:
        addresses={(pair.get('baseToken') or {}).get('address','').lower(),(pair.get('quoteToken') or {}).get('address','').lower()}
        if pair.get('chainId')=='robinhood' and asset.lower() in addresses:pairs.append(pair)
    return max(pairs,key=lambda p:float((p.get('liquidity') or {}).get('usd') or 0)) if pairs else None


def normalize_long(db,rpc,asset,launch):
    values=json.loads(launch['decoded']);quote=values.get('numeraire')
    token=metadata(db,rpc,asset)
    if token.get('decimals') is None:return False
    pool=indexed_pool(asset)
    if not pool:return False
    quote_token=pool.get('quoteToken') or {};change=pool.get('priceChange') or {};volume=pool.get('volume') or {}
    stamp=now();price_quote=float(pool['priceNative']) if pool.get('priceNative') else None
    price_usd=float(pool['priceUsd']) if pool.get('priceUsd') else None
    mc_usd=float(pool.get('marketCap') or pool.get('fdv') or 0) or None
    liq_usd=float((pool.get('liquidity') or {}).get('usd') or 0) or None
    changes=[change.get(k) for k in ('m5','h1','h6','h24')]
    row=(asset,stamp,token['symbol'],token['name'],token['decimals'],quote,quote_token.get('symbol'),None,price_quote,price_usd,None,mc_usd,None,liq_usd,None,None,None,*changes,'long-airlock+dexscreener','indexed-market',None)
    with db:
        db.execute('INSERT OR REPLACE INTO market_snapshots VALUES('+','.join('?'*24)+')',row)
        db.execute('INSERT OR REPLACE INTO market_observations VALUES(?,?,?,?,?,?)',(asset,stamp,price_quote,liq_usd,float(volume.get('m5') or 0),changes[0]))
    return True


def normalize_asset(db,rpc,asset):
    launch=db.execute("SELECT decoded FROM events WHERE asset=? AND name='TokenLaunched' ORDER BY block_number LIMIT 1",(asset,)).fetchone()
    if not launch:
        long_launch=db.execute("SELECT decoded FROM events WHERE asset=? AND kind='long' AND name='Create' ORDER BY block_number LIMIT 1",(asset,)).fetchone()
        return normalize_long(db,rpc,asset,long_launch) if long_launch else False
    watch=db.execute("SELECT address FROM watches WHERE asset=? AND kind='curve' ORDER BY created_block LIMIT 1",(asset,)).fetchone()
    if not launch or not watch:return False
    quote=json.loads(launch['decoded']).get('pairToken')
    if not quote:return False
    token=metadata(db,rpc,asset)
    quote_meta={'symbol':'ETH','decimals':18} if quote.lower()==ZERO else metadata(db,rpc,quote)
    if token.get('decimals') is None or quote_meta.get('decimals') is None:return False
    rows=db.execute("SELECT event_timestamp,decoded FROM events WHERE asset=? AND name IN ('CurveBuy','CurveSell') ORDER BY block_number,log_index",(asset,)).fetchall()
    metrics=calculate(rows,token['decimals'],quote_meta['decimals'],int(time.time()))
    if not metrics:return False
    reserve_raw=uint(rpc.call('eth_getBalance',[watch['address'],'latest'])) if quote.lower()==ZERO else uint(call(rpc,quote,BALANCE+'0'*24+watch['address'][2:]))
    liquidity=reserve_raw/(10**quote_meta['decimals']) if reserve_raw is not None else None
    supply=int(token['total_supply'])/(10**token['decimals']) if token.get('total_supply') else None
    mc=metrics['price_quote']*supply if supply is not None else None
    stamp=now()
    source='onchain-curve-events+native-balance' if quote.lower()==ZERO else 'onchain-curve-events+erc20-balance'
    price_quote,price_usd,mc_usd,liq_usd=metrics['price_quote'],None,None,None
    changes=[metrics['change_5m'],metrics['change_1h'],metrics['change_6h'],metrics['change_24h']]
    graduated=db.execute("SELECT 1 FROM events WHERE asset=? AND name IN ('CurveCompleted','LaunchSwept') LIMIT 1",(asset,)).fetchone()
    if graduated:
        try:
            pool=indexed_pool(asset)
            if pool:
                price_quote=float(pool.get('priceNative') or price_quote)
                price_usd=float(pool['priceUsd']) if pool.get('priceUsd') else None
                mc_usd=float(pool.get('marketCap') or pool.get('fdv') or 0) or None
                liq_usd=float((pool.get('liquidity') or {}).get('usd') or 0) or None
                change=pool.get('priceChange') or {};changes=[change.get(k) for k in ('m5','h1','h6','h24')]
                source='onchain-graduation+dexscreener-v4'
                quote_meta={'symbol':(pool.get('quoteToken') or {}).get('symbol') or quote_meta['symbol'],'decimals':quote_meta['decimals']}
        except (OSError,ValueError,json.JSONDecodeError) as exc:LOG.warning('graduated pool %s delayed: %s',asset,type(exc).__name__)
    values=(asset,stamp,token['symbol'],token['name'],token['decimals'],quote,quote_meta['symbol'],quote_meta['decimals'],price_quote,price_usd,mc,mc_usd,liquidity,liq_usd,metrics['volume_5m_quote'],metrics['volume_1h_quote'],metrics['volume_24h_quote'],*changes,source,'indexed-market' if 'dexscreener' in source else 'quote-only',None)
    with db:
        db.execute('INSERT OR REPLACE INTO market_snapshots VALUES('+','.join('?'*24)+')',values)
        db.execute('INSERT OR REPLACE INTO market_observations VALUES(?,?,?,?,?,?)',(asset,stamp,price_quote,liq_usd if liq_usd is not None else liquidity,metrics['volume_5m_quote'],changes[0]))
        db.execute("DELETE FROM market_observations WHERE strftime('%s',observed_at)<strftime('%s','now','-2 days')")
    return True


def cycle(db,rpc,limit=25):
    head=int(dict(db.execute('SELECT key,value FROM meta')).get('head',0))
    assets=[r[0] for r in db.execute("SELECT asset FROM events WHERE name IN ('Create','CurveBuy','CurveSell') AND block_number>? GROUP BY asset ORDER BY COUNT(*) DESC LIMIT ?",(head-10000,limit))]
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'alerts' in tables:
        tracked=[r[0] for r in db.execute("SELECT DISTINCT asset FROM alerts WHERE rule IN ('onchain-flow-breakout','smart-money-consensus','smart-wallet-entry','repeat-qualified-flow','capital-rotation','coordinated-flow','trench-candidate','momentum-watch','early-watch') ORDER BY id DESC LIMIT ?",(limit,))]
        assets=list(dict.fromkeys(tracked+assets))[:limit*2]
    ok=0
    for asset in assets:
        try:ok+=normalize_asset(db,rpc,asset)
        except (RpcError,sqlite3.Error,ValueError) as exc:LOG.warning('market %s delayed: %s',asset,exc)
    with db:set_meta(db,'market_heartbeat',now());set_meta(db,'market_assets',ok)
    return ok


def main():
    p=argparse.ArgumentParser();p.add_argument('--db',default='data/live.sqlite');p.add_argument('--interval',type=int,default=60);p.add_argument('--limit',type=int,default=25);a=p.parse_args()
    url=os.environ.get('MARKET_RPC_HTTP_URL') or os.environ.get('ENRICHMENT_RPC_HTTP_URL') or os.environ.get('RPC_HTTP_URL')
    if not url:raise ValueError('MARKET_RPC_HTTP_URL or RPC_HTTP_URL required')
    db=database(a.db);schema(db);rpc=RPC(url,attempts=2,spacing=.15)
    try:
        while True:
            LOG.info('normalized assets=%s',cycle(db,rpc,a.limit));time.sleep(max(10,a.interval))
    finally:db.close()


if __name__=='__main__':logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s');main()
