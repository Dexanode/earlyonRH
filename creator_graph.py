"""Attribute Pons v2/Long creators and build a bounded ERC-20 insider supply graph."""
import argparse, json, logging, os, sqlite3, time
from collections import defaultdict, deque
from listener import RPC, RpcError, RateLimited, database, get_meta, now, set_meta

LOG=logging.getLogger('creator-graph')
ZERO='0x'+'0'*40
TRANSFER='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

def schema(db):
    db.executescript('''
    CREATE TABLE IF NOT EXISTS asset_creators(asset TEXT PRIMARY KEY,protocol TEXT NOT NULL,creator TEXT,launch_tx TEXT NOT NULL,launch_block INTEGER NOT NULL,attribution TEXT NOT NULL,confidence TEXT NOT NULL,updated_at TEXT NOT NULL,error TEXT);
    CREATE TABLE IF NOT EXISTS token_transfers(asset TEXT NOT NULL,tx_hash TEXT NOT NULL,log_index INTEGER NOT NULL,block_number INTEGER NOT NULL,from_wallet TEXT NOT NULL,to_wallet TEXT NOT NULL,amount_raw TEXT NOT NULL,PRIMARY KEY(asset,tx_hash,log_index));
    CREATE INDEX IF NOT EXISTS token_transfers_asset ON token_transfers(asset,block_number);
    CREATE TABLE IF NOT EXISTS supply_graph_cursors(asset TEXT PRIMARY KEY,next_block INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS insider_edges(asset TEXT NOT NULL,source TEXT NOT NULL,target TEXT NOT NULL,depth INTEGER NOT NULL,amount_raw TEXT NOT NULL,first_block INTEGER NOT NULL,first_tx TEXT NOT NULL,PRIMARY KEY(asset,source,target));
    CREATE TABLE IF NOT EXISTS insider_wallets(asset TEXT NOT NULL,wallet TEXT NOT NULL,depth INTEGER NOT NULL,reason TEXT NOT NULL,received_raw TEXT NOT NULL,sent_raw TEXT NOT NULL,balance_raw TEXT NOT NULL,sell_count INTEGER NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(asset,wallet));
    ''')

def address_topic(value):
    return '0x'+value[-40:].lower()

def attribute_creators(db,rpc,limit=20):
    rows=db.execute("""SELECT e.asset,e.kind,e.tx_hash,e.block_number,e.decoded FROM events e
      LEFT JOIN asset_creators c ON c.asset=e.asset
      WHERE e.name IN ('TokenLaunched','Create') AND e.kind IN ('pons_v2','long') AND e.asset IS NOT NULL AND c.asset IS NULL
      ORDER BY e.block_number DESC LIMIT ?""",(limit,)).fetchall()
    count=0
    for r in rows:
        creator=error=None;method='factory-event' if r['kind']=='pons_v2' else 'transaction-sender';confidence='high' if r['kind']=='pons_v2' else 'medium'
        if r['kind']=='pons_v2': creator=json.loads(r['decoded']).get('deployer')
        else:
            try:
                tx=rpc.call('eth_getTransactionByHash',[r['tx_hash']]);creator=(tx or {}).get('from')
            except RateLimited: break
            except RpcError as exc:error=str(exc)
        creator=creator.lower() if creator else None
        with db:db.execute('INSERT OR REPLACE INTO asset_creators VALUES(?,?,?,?,?,?,?,?,?)',(r['asset'],r['kind'],creator,r['tx_hash'],r['block_number'],method,confidence,now(),error))
        count+=1
    return count

def active_assets(db,limit=12):
    head=int(get_meta(db,'head') or 0)
    return db.execute("""SELECT c.asset,c.launch_block FROM asset_creators c LEFT JOIN events e ON e.asset=c.asset
      WHERE c.creator IS NOT NULL AND c.error IS NULL AND (e.block_number>? OR c.launch_block>?)
      GROUP BY c.asset ORDER BY SUM(e.name IN ('CurveBuy','DexBuy')) DESC,c.launch_block DESC LIMIT ?""",(head-5000,head-5000,limit)).fetchall()

def ingest_transfers(db,rpc,asset,launch,head,chunk=500):
    row=db.execute('SELECT next_block FROM supply_graph_cursors WHERE asset=?',(asset,)).fetchone();start=row[0] if row else launch
    if start>head:return 0
    end=min(head,start+chunk-1)
    logs=rpc.call('eth_getLogs',[{'address':asset,'fromBlock':hex(start),'toBlock':hex(end),'topics':[TRANSFER]}])
    with db:
        for x in logs:
            if len(x.get('topics',[]))<3:continue
            db.execute('INSERT OR IGNORE INTO token_transfers VALUES(?,?,?,?,?,?,?)',(asset,x['transactionHash'],int(x['logIndex'],16),int(x['blockNumber'],16),address_topic(x['topics'][1]),address_topic(x['topics'][2]),str(int(x.get('data','0x0'),16))))
        db.execute('INSERT OR REPLACE INTO supply_graph_cursors VALUES(?,?)',(asset,end+1))
    return len(logs)

def rebuild_asset(db,asset,creator,max_depth=2):
    rows=db.execute('SELECT * FROM token_transfers WHERE asset=? ORDER BY block_number,log_index',(asset,)).fetchall()
    outgoing=defaultdict(list);received=defaultdict(int);sent=defaultdict(int)
    for r in rows:
        amount=int(r['amount_raw']);outgoing[r['from_wallet']].append(r);received[r['to_wallet']]+=amount;sent[r['from_wallet']]+=amount
    depths={creator:0};q=deque([creator]);edges={}
    while q:
        source=q.popleft();depth=depths[source]
        if depth>=max_depth:continue
        for r in outgoing.get(source,[]):
            target=r['to_wallet'];
            if target==ZERO or target==source:continue
            key=(source,target);old=edges.get(key)
            if old:old['amount']+=int(r['amount_raw'])
            else:edges[key]={'depth':depth+1,'amount':int(r['amount_raw']),'block':r['block_number'],'tx':r['tx_hash']}
            if target not in depths:depths[target]=depth+1;q.append(target)
    attrs={r['tx_hash']:r['sender'] for r in db.execute('SELECT tx_hash,sender FROM tx_attributions WHERE asset=? AND error IS NULL',(asset,))} if db.execute("SELECT 1 FROM sqlite_master WHERE name='tx_attributions'").fetchone() else {}
    sellers=defaultdict(int)
    for r in db.execute("SELECT tx_hash,name,decoded FROM events WHERE asset=? AND name IN ('CurveSell','DexSell')",(asset,)):
        v=json.loads(r['decoded']);wallet=(attrs.get(r['tx_hash']) if r['name']=='DexSell' else None) or v.get('seller')
        if wallet:sellers[wallet.lower()]+=1
    stamp=now()
    with db:
        db.execute('DELETE FROM insider_edges WHERE asset=?',(asset,));db.execute('DELETE FROM insider_wallets WHERE asset=?',(asset,))
        for (source,target),e in edges.items():db.execute('INSERT INTO insider_edges VALUES(?,?,?,?,?,?,?)',(asset,source,target,e['depth'],str(e['amount']),e['block'],e['tx']))
        for wallet,depth in depths.items():db.execute('INSERT INTO insider_wallets VALUES(?,?,?,?,?,?,?,?,?)',(asset,wallet,depth,'creator' if depth==0 else f'creator-transfer-depth-{depth}',str(received[wallet]),str(sent[wallet]),str(max(0,received[wallet]-sent[wallet])),sellers[wallet],stamp))
    return len(depths)

def cycle(db,rpc):
    schema(db);attributed=attribute_creators(db,rpc);logs=wallets=0
    for r in active_assets(db):
        creator=db.execute('SELECT creator FROM asset_creators WHERE asset=?',(r['asset'],)).fetchone()[0];wallets+=rebuild_asset(db,r['asset'],creator)
    with db:set_meta(db,'creator_graph_heartbeat',now());set_meta(db,'creator_assets',db.execute('SELECT COUNT(*) FROM asset_creators WHERE creator IS NOT NULL').fetchone()[0]);set_meta(db,'insider_wallets',db.execute('SELECT COUNT(*) FROM insider_wallets').fetchone()[0])
    return attributed,logs,wallets

def main():
    p=argparse.ArgumentParser();p.add_argument('--db',default='data/live.sqlite');p.add_argument('--interval',type=int,default=45);a=p.parse_args();url=os.environ.get('ENRICHMENT_RPC_HTTP_URL') or os.environ.get('RPC_HTTP_URL')
    if not url:raise ValueError('RPC_HTTP_URL required')
    db=database(a.db);rpc=RPC(url,attempts=1,spacing=.3);schema(db)
    try:
        while True:
            try:LOG.info('creator graph attributed=%s logs=%s wallets=%s',*cycle(db,rpc))
            except sqlite3.Error as exc:LOG.warning('cycle delayed: %s',exc)
            time.sleep(max(20,a.interval))
    finally:db.close()
if __name__=='__main__':logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s');main()
