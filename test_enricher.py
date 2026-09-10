import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from enricher import schema, analyze_asset, attribute_transactions, word_address, ZERO
from listener import database
from test_listener import addr, h


class EnricherTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.db=database(Path(self.tmp.name)/'e.sqlite'); schema(self.db)
        launch=json.dumps({'deployer':addr(9)})
        buy=json.dumps({'buyer':addr(8)})
        with self.db:
            self.db.execute('INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',(h(1),0,10,h(10),addr(4),'pons_v2','TokenLaunched',addr(1),'x',1,launch,'{}',None))
            self.db.execute('INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',(h(2),0,11,h(11),addr(2),'curve','CurveBuy',addr(1),'x',1,buy,'{}',None))
    def tearDown(self): self.db.close(); self.tmp.cleanup()

    def test_address_word_validation(self):
        self.assertEqual(word_address('0x'+'0'*24+addr(1)[2:]),addr(1))
        self.assertIsNone(word_address('0x'+'0'*64)); self.assertIsNone(word_address('bad'))

    def test_analysis_unknowns_do_not_earn_safety_points(self):
        def call(method,params):
            if method=='eth_getCode': return '0x6000'
            if method=='eth_getStorageAt': return '0x'+'0'*64
            if method=='eth_call':
                data=params[0]['data']
                if data=='0x5c975abb': return '0x'+'0'*64
                if data=='0x18160ddd': return hex(100)
                if data.startswith('0x70a08231'): return hex(10)
                return '0x'
            raise AssertionError(method)
        analyze_asset(self.db,Mock(call=call),addr(1))
        row=self.db.execute('SELECT * FROM asset_analysis').fetchone()
        self.assertEqual(row['safety_score'],80)
        self.assertEqual(row['safety_status'],'screened')
        self.assertIn('owner_unknown_or_renounced',json.loads(row['findings']))

    def test_sender_attribution_direct_and_cached(self):
        rpc=Mock();rpc.call.return_value={'from':addr(8)}
        self.assertEqual(attribute_transactions(self.db,rpc,[addr(1)],10),1)
        self.assertEqual(self.db.execute('SELECT relation FROM tx_attributions').fetchone()[0],'direct')
        self.assertEqual(attribute_transactions(self.db,rpc,[addr(1)],10),0)
        self.assertEqual(rpc.call.call_count,1)

if __name__=='__main__': unittest.main()
