import json,tempfile,unittest
from pathlib import Path
from capital_flow import rebuild
from listener import database,now

class CapitalFlowTests(unittest.TestCase):
 def test_repeat_size_inventory_and_shared_sender(self):
  with tempfile.TemporaryDirectory() as d:
   db=database(Path(d)/'x.sqlite');asset='0x'+'a'*40;wallet='0x'+'1'*40;sender='0x'+'f'*40
   db.execute('CREATE TABLE tx_attributions(tx_hash TEXT PRIMARY KEY,asset TEXT,sender TEXT,event_actor TEXT,relation TEXT,checked_at TEXT,error TEXT)')
   values=[('CurveBuy',{'buyer':wallet,'quoteIn':'100','tokensOut':'1000'}),('CurveBuy',{'buyer':wallet,'quoteIn':'200','tokensOut':'900'}),('CurveSell',{'seller':wallet,'quoteOut':'80','tokensIn':'500'})]
   with db:
    for i,(name,v) in enumerate(values):
     tx='0x'+str(i+1).zfill(64);db.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(tx,i,10+i,'0x'+'0'*64,asset,'curve',name,asset,now(),100+i*20,json.dumps(v),'{}',None))
     db.execute('INSERT INTO tx_attributions VALUES(?,?,?,?,?,?,?)',(tx,asset,sender,wallet,'routed',now(),None))
   rebuild(db);row=db.execute('SELECT * FROM capital_wallet_asset').fetchone()
   self.assertEqual((row['buy_count'],row['sell_count'],row['repeat_latency_seconds'],row['size_trend'],row['retained_raw'],row['funding_root']),(2,1,20,2.0,'1400',sender))
   self.assertEqual(db.execute('SELECT confidence FROM capital_clusters').fetchone()[0],'observed-shared-sender');db.close()
if __name__=='__main__':unittest.main()
