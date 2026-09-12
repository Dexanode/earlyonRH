import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from listener import database
from reconciler import launch_lifecycle, reconcile_asset, schema, values


class ReconcilerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.db=database(Path(self.tmp.name)/'x.sqlite');schema(self.db)
    def tearDown(self):self.db.close();self.tmp.cleanup()

    def test_flattens_nested_gmgn_values(self):
        result=values({'token':{'symbol':'GOMO','name':'gomo'},'market':{'price_usd':'0.2','market_cap':'20000','liquidity':'5000'},'holder_count':42})
        self.assertEqual((result['symbol'],result['market_cap_usd'],result['holder_count']),('GOMO',20000.0,42.0))

    @patch('reconciler.gmgn')
    def test_reconciles_identity_and_market(self,gmgn):
        gmgn.side_effect=[{'symbol':'ITEMS','name':'ItemBase','price_usd':'.01','market_cap':'10000'},{'liquidity_usd':'2500'},{'risk_level':'low'}]
        self.assertTrue(reconcile_asset(self.db,'0xabc'))
        row=self.db.execute('SELECT * FROM gmgn_reconciliation').fetchone()
        self.assertEqual((row['symbol'],row['name'],row['market_cap_usd']),('ITEMS','ItemBase',10000))
        meta=self.db.execute('SELECT symbol,name FROM token_metadata').fetchone()
        self.assertEqual(tuple(meta),('ITEMS','ItemBase'))

    def test_pons_lifecycle_tracks_progress_and_migration(self):
        asset='0x'+'1'*40;creator='0x'+'2'*40;stamp=int(dt.datetime.now(dt.timezone.utc).timestamp())
        rows=[
          ('0x'+'a'*64,0,100,'pons_v2','TokenLaunched',stamp,{'token':asset,'deployer':creator,'graduationThreshold':'1000'}),
          ('0x'+'b'*64,0,101,'curve','CurveBuy',stamp+5,{'buyer':'0x'+'3'*40,'quoteIn':'250','tokensOut':'1'}),
          ('0x'+'c'*64,0,102,'curve','CurveSell',stamp+10,{'seller':'0x'+'3'*40,'quoteOut':'50','tokensIn':'1'}),
          ('0x'+'d'*64,0,103,'pons_v2','LaunchSwept',stamp+20,{'token':asset}),
        ]
        with self.db:
            for tx,index,block,kind,name,ts,decoded in rows:
                self.db.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(tx,index,block,'0x'+'0'*64,'0x'+'4'*40,kind,name,asset,'now',ts,json.dumps(decoded),'{}',None))
        self.assertEqual(launch_lifecycle(self.db),1)
        row=self.db.execute('SELECT * FROM launchpad_lifecycle').fetchone()
        self.assertEqual((row['stage'],row['seconds_to_first_buy'],row['curve_progress_pct']),('migrated',5,20.0))


if __name__=='__main__':unittest.main()
