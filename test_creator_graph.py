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
if __name__=='__main__':unittest.main()
