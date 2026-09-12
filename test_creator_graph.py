import json,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock
from creator_graph import schema,attribute_creators,ingest_transfers,rebuild_asset,TRANSFER,ZERO
from listener import database
from test_listener import addr,h

class CreatorGraphTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.db=database(Path(self.tmp.name)/'g.sqlite');schema(self.db)
 def tearDown(self):self.db.close();self.tmp.cleanup()
 def event(self,kind,asset,tx,block,decoded):
  self.db.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(tx,0,block,h(block),addr(99),kind,'TokenLaunched' if kind=='pons_v2' else 'Create',asset,'x',1,json.dumps(decoded),'{}',None))
 def test_creator_attribution_for_pons_and_long(self):
  self.event('pons_v2',addr(1),h(1),10,{'deployer':addr(8)});self.event('long',addr(2),h(2),11,{'asset':addr(2)})
  rpc=Mock();rpc.call.return_value={'from':addr(9)}
  self.assertEqual(attribute_creators(self.db,rpc),2)
  rows={r['asset']:dict(r) for r in self.db.execute('SELECT * FROM asset_creators')}
  self.assertEqual((rows[addr(1)]['creator'],rows[addr(1)]['confidence']),(addr(8),'high'))
  self.assertEqual((rows[addr(2)]['creator'],rows[addr(2)]['attribution']),(addr(9),'transaction-sender'))
 def test_transfer_graph_marks_creator_linked_seller(self):
  creator,insider=addr(8),addr(7);asset=addr(1)
  with self.db:
   self.db.execute('INSERT INTO token_transfers VALUES(?,?,?,?,?,?,?)',(asset,h(3),0,12,creator,insider,'400'))
   self.db.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(h(4),0,13,h(13),addr(4),'curve','CurveSell',asset,'x',1,json.dumps({'seller':insider}),'{}',None))
  self.assertEqual(rebuild_asset(self.db,asset,creator),2)
  row=self.db.execute('SELECT depth,balance_raw,sell_count FROM insider_wallets WHERE asset=? AND wallet=?',(asset,insider)).fetchone()
  self.assertEqual(tuple(row),(1,'400',1))
 def test_transfer_ingestion_advances_bounded_cursor(self):
  asset=addr(1);creator=addr(8);insider=addr(7)
  log={'transactionHash':h(3),'logIndex':'0x0','blockNumber':'0xa','topics':[TRANSFER,'0x'+'0'*24+creator[2:],'0x'+'0'*24+insider[2:]],'data':hex(25)}
  rpc=Mock();rpc.call.return_value=[log]
  self.assertEqual(ingest_transfers(self.db,rpc,asset,10,999,500),1)
  self.assertEqual(self.db.execute('SELECT next_block FROM supply_graph_cursors').fetchone()[0],510)
 def test_bundle_detector_combines_linked_same_block_buyers(self):
  asset,creator=addr(1),addr(8)
  with self.db:
   self.db.execute('INSERT INTO asset_creators VALUES(?,?,?,?,?,?,?,?,?)',(asset,'pons_v2',creator,h(1),10,'factory-event','high','x',None))
   self.db.execute('INSERT INTO token_transfers VALUES(?,?,?,?,?,?,?)',(asset,h(10),0,10,ZERO,creator,'1000'))
   for i,wallet in enumerate((addr(4),addr(5),addr(6)),1):
    self.db.execute('INSERT INTO token_transfers VALUES(?,?,?,?,?,?,?)',(asset,h(10+i),0,10,creator,wallet,'200'))
    self.db.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(h(20+i),0,11,h(11),addr(3),'curve','CurveBuy',asset,'x',1,json.dumps({'buyer':wallet,'quoteIn':'100','tokensOut':'10'}),'{}',None))
  rebuild_asset(self.db,asset,creator)
  row=self.db.execute('SELECT classification,bundle_score,creator_linked_early_buyers,same_block_buyers FROM distribution_analysis').fetchone()
  self.assertEqual(row['classification'],'possible-bundled-launch');self.assertGreaterEqual(row['bundle_score'],55);self.assertEqual((row['creator_linked_early_buyers'],row['same_block_buyers']),(3,3))
if __name__=='__main__':unittest.main()
