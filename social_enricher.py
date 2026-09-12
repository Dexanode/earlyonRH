"""Conservative social identity graph from public token metadata and project pages."""
import argparse,html.parser,ipaddress,json,logging,socket,sqlite3,time
from urllib import request,parse,error
from listener import database,now,set_meta
LOG=logging.getLogger('social-enricher');DEX='https://api.dexscreener.com/latest/dex/tokens/'
class Links(html.parser.HTMLParser):
 def __init__(self):super().__init__();self.links=[]
 def handle_starttag(self,tag,attrs):
  if tag=='a':
   href=dict(attrs).get('href')
   if href:self.links.append(href)
def safe_url(value):
 try:
  u=parse.urlparse(value);host=(u.hostname or '').lower()
  if u.scheme not in ('http','https') or not host:return None
  try:
   if ipaddress.ip_address(host).is_private:return None
  except ValueError:pass
  if host in ('localhost','localhost.localdomain'):return None
  return u.geturl()
 except ValueError:return None
def normalize(url):
 u=parse.urlparse(url);host=(u.hostname or '').lower().removeprefix('www.');path=u.path.rstrip('/')
 return host+path.lower()
def extract(pair):
 info=pair.get('info') or {};sites=[];socials={}
 for x in info.get('websites') or []:
  u=safe_url(x.get('url'));sites.extend([u] if u else [])
 for x in info.get('socials') or []:
  u=safe_url(x.get('url'));kind=(x.get('type') or parse.urlparse(u or '').hostname or '').lower()
  if u:socials[kind]=u
 return list(dict.fromkeys(sites)),socials
def fetch_json(url,timeout=10):
 with request.urlopen(request.Request(url,headers={'User-Agent':'earlyonRH/1','Accept':'application/json'}),timeout=timeout) as r:return json.load(r)
def fetch_links(url,timeout=8):
 # Resolve before fetching to reject obvious private-network targets.
 host=parse.urlparse(url).hostname
 if any(ipaddress.ip_address(x[4][0]).is_private for x in socket.getaddrinfo(host,443,type=socket.SOCK_STREAM)):raise ValueError('private destination')
 with request.urlopen(request.Request(url,headers={'User-Agent':'earlyonRH-social/1'}),timeout=timeout) as r:body=r.read(300000).decode('utf-8','ignore')
 p=Links();p.feed(body);return [parse.urljoin(url,x) for x in p.links]
def schema(db):
 db.executescript('''CREATE TABLE IF NOT EXISTS social_identity(asset TEXT PRIMARY KEY,updated_at TEXT NOT NULL,website TEXT,x_url TEXT,telegram_url TEXT,discord_url TEXT,source_count INTEGER NOT NULL,website_links_social INTEGER NOT NULL,cross_linked INTEGER NOT NULL,score REAL NOT NULL,confidence TEXT NOT NULL,status TEXT NOT NULL,evidence TEXT NOT NULL,error TEXT);CREATE TABLE IF NOT EXISTS social_links(asset TEXT NOT NULL,kind TEXT NOT NULL,url TEXT NOT NULL,normalized TEXT NOT NULL,source TEXT NOT NULL,verified INTEGER NOT NULL,PRIMARY KEY(asset,kind,url));''')
def analyze(db,asset):
 err=None
 try:
  payload=fetch_json(DEX+asset);pairs=[p for p in payload.get('pairs') or [] if p.get('chainId')=='robinhood'];pair=max(pairs,key=lambda p:float((p.get('liquidity') or {}).get('usd') or 0)) if pairs else {};sites,socials=extract(pair);website=sites[0] if sites else None
  x_url=next((v for k,v in socials.items() if k in ('twitter','x','x.com')),None);tg=next((v for k,v in socials.items() if 'telegram' in k),None);dc=next((v for k,v in socials.items() if 'discord' in k),None)
  linked=False;page=[]
  if website:
   try:page=fetch_links(website);targets={normalize(u) for u in (x_url,tg,dc) if u};linked=bool(targets & {normalize(u) for u in page if safe_url(u)})
   except (OSError,ValueError):pass
  source_count=int(bool(pair))+int(bool(website));score=min(100,20*bool(website)+20*bool(x_url)+10*bool(tg or dc)+35*linked+15*(len(sites)>1));cross=bool(website and x_url and linked);confidence='high' if cross else 'medium' if website and x_url else 'low';status='cross-linked' if cross else 'socials-present' if website or socials else 'no-social-evidence';evidence={'dex_pair':pair.get('pairAddress'),'page_links_checked':bool(website),'matched_from_website':linked,'method':'DexScreener metadata + project-page outbound link'}
 except (OSError,ValueError,json.JSONDecodeError) as exc:website=x_url=tg=dc=None;source_count=score=0;cross=linked=False;confidence='low';status='fetch-delayed';evidence={};err=type(exc).__name__
 with db:
  db.execute('INSERT OR REPLACE INTO social_identity VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(asset,now(),website,x_url,tg,dc,source_count,int(linked),int(cross),score,confidence,status,json.dumps(evidence),err));db.execute('DELETE FROM social_links WHERE asset=?',(asset,))
  for kind,url in [('website',website),('x',x_url),('telegram',tg),('discord',dc)]:
   if url:db.execute('INSERT INTO social_links VALUES(?,?,?,?,?,?)',(asset,kind,url,normalize(url),'dexscreener',int(linked and kind!='website')))
def cycle(db,limit=20):
 schema(db);assets=[r[0] for r in db.execute("""SELECT m.asset FROM market_snapshots m LEFT JOIN social_identity s ON s.asset=m.asset
   WHERE m.status!='unknown' ORDER BY s.updated_at IS NULL DESC,s.updated_at ASC,m.updated_at DESC LIMIT ?""",(limit,))]
 for a in assets:
  try:analyze(db,a)
  except sqlite3.Error as exc:LOG.warning('social delayed %s %s',a,exc)
 with db:set_meta(db,'social_heartbeat',now());set_meta(db,'social_assets',db.execute("SELECT COUNT(*) FROM social_identity WHERE status!='fetch-delayed'").fetchone()[0]);set_meta(db,'social_cross_linked',db.execute('SELECT COUNT(*) FROM social_identity WHERE cross_linked=1').fetchone()[0])
 return len(assets)
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',default='data/live.sqlite');p.add_argument('--interval',type=int,default=300);p.add_argument('--limit',type=int,default=20);a=p.parse_args();db=database(a.db);schema(db)
 try:
  while True:LOG.info('social assets=%s',cycle(db,a.limit));time.sleep(max(120,a.interval))
 finally:db.close()
if __name__=='__main__':logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s');main()
